#!/usr/bin/env python3

import sqlite3
import pathlib
import logging
import time
import flask
from flask_cors import CORS
import requests
import json


class Db:
    ROLES = { 1: "left", 2: "right" }
    METER_NODE = "rest_meter"
    METER_FMT = METER_NODE+":{:03}-{}" # (Stereo Channel, Role)
    def __init__(self):
        self.dbpath = pathlib.Path(__file__).parent.joinpath("matrix.sqlite")
        logging.info(f'Opening database at {self.dbpath} with threadsafety {sqlite3.threadsafety}')
        
        self.RROLES = { name: id for id, name in self.ROLES.items() }
        self.setup_db()
        self.last_ingest = 0.0

    def connect(self):
        conn = sqlite3.connect(self.dbpath, check_same_thread=False, autocommit=False)
        # WTF https://github.com/python/cpython/issues/120530 Enable foreign key constraint enforcement for every damn connection with hackyness
        conn.executescript('COMMIT;PRAGMA foreign_keys = ON;BEGIN')
        return conn

    def setup_db(self):
        with self.connect() as c:
            c.execute("CREATE TABLE IF NOT EXISTS dst(dst INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL, description TEXT)")
            c.execute("CREATE TABLE IF NOT EXISTS src(src INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL, description TEXT)")
            c.execute("CREATE TABLE IF NOT EXISTS patch(src INTEGER REFERENCES src(src) ON DELETE CASCADE, dst INTEGER REFERENCES dst(dst) ON DELETE CASCADE, UNIQUE (dst) )")
            c.execute("CREATE TABLE IF NOT EXISTS matrix(matrix INTEGER PRIMARY KEY, name TEXT UNIQUE, description TEXT, mode INTEGER)")

            c.execute("CREATE TABLE IF NOT EXISTS matrixsrc(matrix INTEGER REFERENCES matrix(matrix) ON DELETE CASCADE, src INTEGER REFERENCES src(src) ON DELETE CASCADE, position INTEGER, UNIQUE (matrix, src) )")
            c.execute("CREATE TABLE IF NOT EXISTS matrixdst(matrix INTEGER REFERENCES matrix(matrix) ON DELETE CASCADE, dst INTEGER REFERENCES dst(dst) ON DELETE CASCADE, position INTEGER, UNIQUE (matrix, dst) )")

            c.execute("CREATE TABLE IF NOT EXISTS dstmap(dst INTEGER REFERENCES dst(dst) ON DELETE CASCADE, role INTEGER NOT NULL, dstport INTEGER UNIQUE REFERENCES dstport(dstport) ON DELETE CASCADE, UNIQUE (dst, role))")
            c.execute("CREATE TABLE IF NOT EXISTS srcmap(src INTEGER REFERENCES src(src) ON DELETE CASCADE, role INTEGER NOT NULL, srcport INTEGER UNIQUE REFERENCES srcport(srcport) ON DELETE CASCADE, UNIQUE (src, role))")

            c.execute("CREATE TABLE IF NOT EXISTS dstport(dstport INTEGER PRIMARY KEY, name TEXT UNIQUE, lastseen REAL)")
            c.execute("CREATE TABLE IF NOT EXISTS srcport(srcport INTEGER PRIMARY KEY, name TEXT UNIQUE, lastseen REAL)")

            c.execute("CREATE TABLE IF NOT EXISTS link(srcport INTEGER REFERENCES srcport(srcport), dstport INTEGER REFERENCES dstport(dstport), UNIQUE (srcport, dstport) )")

            c.execute("CREATE TABLE IF NOT EXISTS meter(meter INTEGER PRIMARY KEY, src INTEGER UNIQUE REFERENCES src(src) ON DELETE CASCADE, lastused REAL)")
    
    def get_matrix_lines(self, id: int):
        with self.connect() as c:
            r = c.execute("SELECT name, description, mode from matrix WHERE matrix = ?", (id,))
            info=r.fetchone()

            srcs = []
            dsts = []
            for dir, lines in (('src', srcs), ('dst',dsts)):
                r = c.execute(f"SELECT {dir}.{dir}, {dir}.name FROM matrix{dir}, {dir} "+
                        f"WHERE matrix{dir}.matrix == ? AND {dir}.{dir} == matrix{dir}.{dir} ORDER BY matrix{dir}.position", (id,))
                lines.extend([ {'id': id, 'name': name } for id, name in r])

            return {'name': info[0], 'description': info[1], 'mode': info[2]}, srcs, dsts

    def get_matrix_active(self, id: int):
        with self.connect() as c:
            r_active = c.execute("SELECT src.src, dst.dst, srcmap.role, dstmap.role FROM matrixsrc, matrixdst, src, dst, srcmap, dstmap, link " +
                    "WHERE matrixsrc.matrix = ? AND matrixdst.matrix = ? AND matrixsrc.src = src.src AND matrixdst.dst = dst.dst " +
                    "AND srcmap.src = src.src AND dstmap.dst = dst.dst AND srcmap.srcport = link.srcport AND dstmap.dstport = link.dstport " +
                    "ORDER BY src.src, dst.dst, srcmap.role",
                    (id, id))
            logging.debug(f"active: {r_active}")
            return r_active.fetchall()

    def get_matrix_desired(self, id: int):
        with self.connect() as c:
            r_desired = c.execute("SELECT src.src, dst.dst, srcmap.role FROM matrixsrc, matrixdst, src, dst, srcmap, dstmap, patch " +
                    "WHERE matrixsrc.matrix = ? AND matrixdst.matrix = ? AND matrixsrc.src = src.src AND matrixdst.dst = dst.dst " +
                    "AND srcmap.src = src.src AND dstmap.dst = dst.dst AND srcmap.role = dstmap.role AND src.src = patch.src AND dst.dst = patch.dst " +
                    "ORDER BY src.src, dst.dst, srcmap.role",
                    (id, id))
            return r_desired.fetchall()


    def get_matrix_patch_state(self, id: int):
        result = {}
        mtx_desired = self.get_matrix_desired(id)
        logging.debug(f"Desired: {mtx_desired}")
        for src, dst, role in mtx_desired:
            if dst not in result:
                result[dst] = {'desired': { 'src': src }}
            
            result[dst]['desired'][self.ROLES[role]] = False

        mtx_active = self.get_matrix_active(id)
        logging.debug(f"Active: {mtx_active}")
        for src, dst, srcrole, dstrole in mtx_active:
            desired = result.get(dst, {}).get('desired',{'src':None})
            if desired['src'] == src and self.ROLES[dstrole] in desired and dstrole == srcrole:
                desired[self.ROLES[dstrole]] = True
                continue

            if dst not in result:
                result[dst] = {'others': []}
            elif 'others' not in result[dst]:
                result[dst]['others'] = []
            m = [ other for other in result[dst]['others'] if other['src'] == src ]
            if m:
                m[0][self.ROLES[dstrole]] = self.ROLES[srcrole]
            else:
                result[dst]['others'].append({'src':src, self.ROLES[dstrole]: self.ROLES[srcrole]})

        logging.debug(f"Patch state: {result}")
        return result

    def get_matrix_meters(self, id: int):
        now = time.time()
        srcselect = (f'SELECT srcmap.src AS s, ? FROM matrixdst, dstmap, srcmap, link ' +
                    'WHERE matrixdst.matrix == ? AND matrixdst.dst == dstmap.dst ' +
                    'AND dstmap.dstport == link.dstport AND srcmap.srcport == link.srcport '+
                    'UNION SELECT src AS s, ? FROM matrixsrc WHERE matrixsrc.matrix == ?')
        with self.connect() as c:
            c.execute(f'INSERT INTO meter (src, lastused) {srcselect} ON CONFLICT DO UPDATE SET lastused = ?', (now, id, now, id, now))
            r = c.execute(f'SELECT s, meter from ({srcselect}) left outer join meter on meter.src == s', (now, id, now, id))

            return r.fetchall()

    def __create_line(self, dir:str, name: str, desc: str, ports: dict):
        logging.debug(f'Create {dir}, "{name}", "{desc}", ports: {ports}')
        with self.connect() as c:
            if type(ports) is not dict or any([role not in self.RROLES or type(port) is not int for role, port in ports.items()]):
                raise KeyError(f'Ports present but incorrectly formatted')
            r = c.execute(f"INSERT INTO {dir} (name, description) VALUES (?, ?)", (name, desc))
            lineid = r.lastrowid
            data = [(lineid, self.RROLES[role], port) for role, port in ports.items()]
            c.executemany(f"INSERT INTO {dir}map ({dir}, role, {dir}port) VALUES (?, ?, ?)", data)
            return lineid

    def create_src(self, name: str, desc: str, ports: dict):
        return self.__create_line('src', name, desc, ports)

    def create_dst(self, name: str, desc: str, ports: dict):
        return self.__create_line('dst', name, desc, ports)

    def __update_line(self, dir:str, id:int, name: str, desc: str, ports: dict):
        with self.connect() as c:
            if ports is not None and any([role not in self.RROLES or type(port) is not int for role, port in ports.items()]):
                raise KeyError(f'Ports present but incorrectly formatted')
            if name is None and desc is None:
                r = c.execute(f"SELECT COUNT(*) FROM {dir} WHERE {dir}.{dir} = ?", (id,))
                if r.fetchone[0] == 0:
                    raise KeyError(f'{dir} id, {id} not found, abandoning')
            if name is not None:
                c.execute(f"UPDATE {dir} SET name = ? WHERE {dir}.{dir} = ?", (name, id))
            if desc is not None:
                c.execute(f"UPDATE {dir} SET description = ? WHERE {dir}.{dir} = ?", (desc, id))
            if ports is not None:
                data = [(id, self.RROLES[role], port) for role, port in ports.items()]
                c.execute(f"DELETE FROM {dir}map WHERE {dir} = ?", (id,))
                c.executemany(f"INSERT INTO {dir}map ({dir}, role, {dir}port) VALUES (?, ?, ?)", data)

    def update_src(self, id:int, name: str|None, desc: str|None, ports: dict|None):
        return self.__update_line('src', id, name, desc, ports)

    def update_dst(self, id:int, name: str|None, desc: str|None, ports: dict|None):
        return self.__update_line('dst', id, name, desc, ports)

    def __del_line(self, dir, id):
        with self.connect() as c:
            # c.execute(f"DELETE FROM {dir}map WHERE {dir} = ?", (id,))
            c.execute(f"DELETE FROM {dir} WHERE {dir}.{dir} = ?", (id,))

    def del_src(self, id):
        self.__del_line('src', id)

    def del_dst(self, id):
        self.__del_line('dst', id)

    def __get_lines(self, dir, id=None):
        with self.connect() as c:
            idfilt = f"WHERE {dir}.{dir} = {id} " if type(id) is int else ""
            lines = []
            r = c.execute(f"SELECT {dir}.{dir}, {dir}.name, {dir}.description, role, pid, pname "+
                    f"FROM {dir} LEFT OUTER JOIN "+
                    f"(SELECT {dir}map.{dir} as lid, role, {dir}port.{dir}port as pid, name as pname FROM {dir}map, {dir}port WHERE {dir}map.{dir}port = {dir}port.{dir}port) "+
                    f"ON lid = {dir}.{dir} {idfilt}" +
                    f"ORDER BY {dir}.{dir}, role")

            for id, name, desc, role, pid, port in r:
                if not lines or id != lines[-1]['id']:
                    lines.append({'id': id,'name': name, 'description': desc, 'ports': {}})
                if port is not None:
                    lines[-1]['ports'][self.ROLES[role]] = [pid, port]

            return lines

    def get_srcs(self, id=None):
        return self.__get_lines('src', id)

    def get_dsts(self, id=None):
        return self.__get_lines('dst', id)

    def create_matrix(self, name: str, desc: str, mode: int, srcs: list, dsts: list):
        with self.connect() as c:
            r = c.execute("INSERT INTO matrix (name, description, mode) VALUES (?, ?, ?)", (name, desc, mode))
            matrixid = r.lastrowid
            for dir, lines in (('src', srcs), ('dst', dsts)):
                data = [ (matrixid, line, n) for n, line in enumerate(lines) ]
                c.executemany(f"INSERT INTO matrix{dir} (matrix, {dir}, position) VALUES (?, ?, ?)", data)
            return matrixid

    def get_matrices(self):
        with self.connect() as c:
            r = c.execute("SELECT matrix, name, description, mode from matrix")
            return r.fetchall()

    def __get_unassigned_ports(self, dir):
        with self.connect() as c:
            r = c.execute(f"SELECT {dir}port.{dir}port, name, {self.last_ingest}-lastseen "+
                          f"FROM {dir}port LEFT OUTER JOIN {dir}map ON {dir}port.{dir}port = {dir}map.{dir}port "+
                          f"WHERE role IS NULL ORDER BY lastseen DESC, name")
            return [{'id': entry[0], 'name': entry[1], 'lastseen': entry[2]} for entry in r]

    def get_unassigned_srcports(self):
        return self.__get_unassigned_ports('src')

    def get_unassigned_dstports(self):
        return self.__get_unassigned_ports('dst')

    def get_port_patches(self):
        with self.connect() as c:
            r_patch = c.execute("SELECT srcport.name, dstport.name FROM srcmap, dstmap, srcport, dstport, patch " +
                    "WHERE srcmap.src == patch.src AND dstmap.dst == patch.dst AND srcmap.role == dstmap.role AND srcmap.srcport == srcport.srcport AND dstmap.dstport == dstport.dstport")
            r_meter = c.execute("SELECT srcport.name, meter.meter, srcmap.role FROM meter, srcmap, srcport WHERE meter.src == srcmap.src AND srcmap.srcport == srcport.srcport and (srcmap.role == 1 OR srcmap.role == 2)")
            return r_patch.fetchall() + [ [portname, self.METER_FMT.format(meter, self.ROLES[role])] for portname, meter, role in r_meter.fetchall() ]

    def update_matrix(self, matrix_id: int, name: str | None, desc: str | None, mode: int | None, new_srcs: list[int] | None, new_dsts: list[int] | None):
        logging.debug(f"Updating matrix {matrix_id}. Name: {name}, Desc: {desc}, Mode: {mode}, Srcs: {new_srcs}, Dsts: {new_dsts}")
        with self.connect() as c:
            if name is not None:
                c.execute("UPDATE matrix SET name = ? WHERE matrix = ?", (name, matrix_id))
            if desc is not None:
                c.execute("UPDATE matrix SET description = ? WHERE matrix = ?", (desc, matrix_id))
            if mode is not None:
                c.execute("UPDATE matrix SET mode = ? WHERE matrix = ?", (mode, matrix_id))

            if new_srcs is not None: # If None, don't touch matrixsrc. If [], clear them.
                logging.debug(f"Updating sources for matrix {matrix_id}. New sources: {new_srcs}")
                c.execute("DELETE FROM matrixsrc WHERE matrix = ?", (matrix_id,))
                if new_srcs:
                    src_data = [(matrix_id, src_id, pos) for pos, src_id in enumerate(new_srcs)]
                    c.executemany("INSERT INTO matrixsrc (matrix, src, position) VALUES (?, ?, ?)", src_data)

            if new_dsts is not None: # If None, don't touch matrixdst. If [], clear them.
                logging.debug(f"Updating destinations for matrix {matrix_id}. New destinations: {new_dsts}")
                c.execute("DELETE FROM matrixdst WHERE matrix = ?", (matrix_id,))
                if new_dsts:
                    dst_data = [(matrix_id, dst_id, pos) for pos, dst_id in enumerate(new_dsts)]
                    c.executemany("INSERT INTO matrixdst (matrix, dst, position) VALUES (?, ?, ?)", dst_data)
            
            logging.info(f"Matrix {matrix_id} updated successfully.")

    def del_matrix(self, matrix_id: int):
        logging.debug(f"Deleting matrix {matrix_id}")
        with self.connect() as c:
            try:
                # Delete parent should cascade to matrixsrc/dst tables
                cursor = c.execute("DELETE FROM matrix WHERE matrix = ?", (matrix_id,))
                
                if cursor.rowcount == 0:
                    logging.warning(f"Attempted to delete matrix {matrix_id}, but it was not found.")
                    # raise KeyError(f"Matrix with id {matrix_id} not found for deletion.") # Optional: be strict
                
                c.commit()
                logging.info(f"Matrix {matrix_id} deleted successfully.")
                return cursor.rowcount > 0 # Return True if a row was deleted
            except sqlite3.Error as e:
                logging.error(f"Database error deleting matrix {matrix_id}: {e}")
                raise

    def patch(self, src: int, dst: int):
        with self.connect() as c:
            c.execute("INSERT OR REPLACE INTO patch (src, dst) VALUES (?, ?)", (src, dst))

    def unpatch(self, dst: int):
        with self.connect() as c:
            c.execute("DELETE FROM patch WHERE dst == ?", (dst,))

    def get_patches(self):
        with self.connect() as c:
            r = c.execute("SELECT src, dst FROM patch")
            return [ {'src': src, 'dst': dst } for src, dst in r ]

    def ingest_state(self, srcs, dsts, links):
        with self.connect() as c:
            now = time.time()
            for dir, ports in (('src', srcs), ('dst', dsts)):
                c.executemany(f"INSERT INTO {dir}port (name, lastseen) VALUES (?, ?) ON CONFLICT DO UPDATE SET lastseen=? ", ( (port, now, now) for port in ports if port[:len(self.METER_NODE)] != self.METER_NODE) )
            c.execute(f"DELETE FROM link")
            c.executemany(f"INSERT INTO link (srcport, dstport) SELECT srcport.srcport, dstport.dstport FROM srcport, dstport WHERE srcport.name == ? AND dstport.name == ? ON CONFLICT DO NOTHING", links)
            inscount = c.execute(f"SELECT COUNT(*) FROM link").fetchone()[0]
            if inscount != len(links):
                logging.warning(f"Ingesting links, only { inscount }, inserts, but { len(links) } links! Is there a duplicate?")
            self.last_ingest = now


class PW:
    def __init__(self, uri='http://127.0.0.1:9080/'):
        self.uri = uri

    def get_ports(self):
        try:
            resp = requests.get(self.uri + 'ports')
            return resp.json()
        except Exception as e:
            logging.exception(f"Exception {e} getting ports from pipewire")
            raise e

    def get_links(self):
        try:
            resp = requests.get(self.uri + 'links/active')
            return resp.json()
        except Exception as e:
            logging.exception(f"Exception {e} getting active links from pipewire")
            raise e
        
    def set_desired(self, desired_links):
        try:
            resp = requests.put(self.uri + 'links/desired', data=json.dumps(desired_links))
            logging.debug(f"Set {len(desired_links)} desired links, got code {resp.status_code}")
            return resp.status_code < 300
        except Exception as e:
            logging.exception(f"Exception {e} getting active links from pipewire")
            raise e


def update_db_from_pw(db, pw):
    srcs, dsts = pw.get_ports()
    links = pw.get_links()
    db.ingest_state(srcs, dsts, links)

def update_pw_from_db(db, pw):
    desired = db.get_port_patches()
    return pw.set_desired(desired)

def api(app, db, pw):
    METER_HOST = 'http://coralpink:9081'
    METER_METHOD = '/levels/map'

    @app.route('/matrix', methods=['GET'])
    def get_matrices():
        matrices = db.get_matrices()
        return [ { 'id': id, 'name': name, 'description': desc, 'mode': mode } for id, name, desc, mode in matrices ]

    @app.route('/matrix/<int:id>', methods=['GET'])
    def get_matrix(id):
        update_db_from_pw(db, pw)
        info, srcs, dsts = db.get_matrix_lines(id)
        patch_state = db.get_matrix_patch_state(id)
        for dst in dsts:
            ps = patch_state.get(dst['id'], None)
            if ps is not None:
                dst.update(ps)
                if 'desired' in ps:
                    if 'others' in ps:
                        dst['state'] = 'badpatch'
                    elif all(ps['desired'].values()):
                        dst['state'] = 'patched'
                    elif any([patched for patched in ps['desired'].values() if type(patched) is bool]):
                        dst['state'] = 'partial'
                    else:
                        dst['state'] = 'inactive'
                elif 'others' in ps:
                        dst['state'] = 'overpatched'
                else:
                        dst['state'] = 'unpatched'
            else:
                    dst['state'] = 'unpatched'

        meter_url = METER_HOST + METER_METHOD + '?' + '&'.join([ f'{src}={meterchan}' for src, meterchan in db.get_matrix_meters(id) ])

        info.update({'srcs': srcs, 'dsts': dsts, 'meterurl': meter_url })
        return info, 200 if srcs or dsts else 404

    @app.route('/matrix/<int:id>/metermap', methods=['GET'])
    def get_matrix_meter(id):
        return db.get_matrix_meters(id)

    @app.route('/levels/map', methods=['GET'])
    def proxy_meter():
        resp = requests.get(METER_HOST + METER_METHOD, params=flask.request.args)
        return resp.text

    @app.route('/matrix', methods=['POST'])
    def create_matrix_endpoint():
        data = flask.request.get_json()
        if not data or 'name' not in data or 'srcs' not in data or 'dsts' not in data:
            return flask.jsonify({"error": "Missing required fields: name, srcs, dsts"}), 400
        
        name = data['name']
        desc = data.get('description', '')
        mode = data.get('mode', 0) # Default mode to 0 if not provided
        srcs_ids = data.get('srcs', []) # List of source IDs in order
        dsts_ids = data.get('dsts', []) # List of destination IDs in order

        if not isinstance(name, str) or not isinstance(desc, str) or \
           not isinstance(mode, int) or not isinstance(srcs_ids, list) or \
           not isinstance(dsts_ids, list):
            return flask.jsonify({"error": "Invalid data types for one or more fields."}), 400
        
        matrix_id = db.create_matrix(name, desc, mode, srcs_ids, dsts_ids)
        return {"id": matrix_id, "name": name, "description": desc, "mode": mode}, 201

    @app.route('/matrix/<int:id>', methods=['PUT'])
    def update_matrix_endpoint(id):
        data = flask.request.get_json()
        if not data:
            return flask.jsonify({"error": "Request body must be JSON."}), 400

        db.update_matrix(
            matrix_id=id,
            name=data.get('name'), # Pass None if key is missing, db method handles it
            desc=data.get('description'),
            mode=data.get('mode'),
            new_srcs=data.get('srcs'), # Pass list of IDs or None
            new_dsts=data.get('dsts')  # Pass list of IDs or None
        )
        return '"ok"\n'

    @app.route('/matrix/<int:id>', methods=['DELETE'])
    def del_matrix_endpoint(id):
        deleted = db.del_matrix(id)
        if deleted:
            return {"message": f"Matrix {id} deleted successfully."}, 200 # Or 204 No Content
        else:
            return {"error": f"Matrix {id} not found."}, 404

    @app.route('/src', methods=['GET'])
    def get_srcs():
        return db.get_srcs()

    @app.route('/src/<int:id>', methods=['GET'])
    def get_src(id):
        return db.get_srcs(id)[0]

    @app.route('/src/<int:id>', methods=['PUT'])
    def update_src(id):
        data = flask.request.get_json()
        db.update_src(id, data.get('name', None), data.get('description', None), data.get('ports', None))
        if 'ports' in data:
            update_pw_from_db(db, pw)
        return '"ok"\n'

    @app.route('/src/<int:id>', methods=['DELETE'])
    def del_src(id):
        db.del_src(id)
        update_pw_from_db(db, pw)
        return '"ok"\n'

    @app.route('/src', methods=['PUT','POST'])
    def create_src():
        data = flask.request.get_json()
        return { 'id': db.create_src(data['name'], data.get('description', ''), data.get('ports', {})) }

    @app.route('/dst', methods=['GET'])
    def get_dsts():
        return db.get_dsts()

    @app.route('/dst/<int:id>', methods=['GET'])
    def get_dst(id):
        return db.get_dsts(id)[0]

    @app.route('/dst/<int:id>', methods=['PUT'])
    def update_dst(id):
        data = flask.request.get_json()
        db.update_dst(id, data.get('name', None), data.get('description', None), data.get('ports', None))
        if 'ports' in data:
            update_pw_from_db(db, pw)
        return '"ok"\n'

    @app.route('/dst/<int:id>', methods=['DELETE'])
    def del_dst(id):
        db.del_dst(id)
        update_pw_from_db(db, pw)
        return '"ok"\n'

    @app.route('/dst', methods=['PUT', 'POST'])
    def create_dst():
        data = flask.request.get_json()
        return { 'id': db.create_dst(data['name'], data.get('description', ''), data.get('ports', {})) }

    @app.route('/port', methods=['GET'])
    def get_ports():
        srcs, dsts = pw.get_ports()
        return {
            'srcs': srcs,
            'dsts': dsts
        }

    @app.route('/port/src/unassigned', methods=['GET'])
    def get_unassigned_srcports():
        update_db_from_pw(db, pw)
        return db.get_unassigned_srcports()

    @app.route('/port/dst/unassigned', methods=['GET'])
    def get_unassigned_dstports():
        update_db_from_pw(db, pw)
        return db.get_unassigned_dstports()

    @app.route('/patch', methods=['GET'])
    def get_patches():
        return db.get_patches()

    @app.route('/patch/<int:dst>/<int:src>', methods=['PUT'])
    def create_patch(dst, src):
        db.patch(src, dst)
        ok = update_pw_from_db(db, pw)
        return ('"ok"\n', 200) if ok else ('"Failed to set desired links"', 500)

    @app.route('/patch/<int:dst>', methods=['DELETE'])
    def del_patch(dst):
        db.unpatch(dst)
        ok = update_pw_from_db(db, pw)
        return ('"ok"\n', 200) if ok else ('"Failed to unset desired links"', 500)

    @app.route('/sync', methods=['POST'])
    def sync_now():
        update_db_from_pw(db, pw)
        ok = update_pw_from_db(db, pw)
        time.sleep(0.1)
        update_db_from_pw(db, pw)
        return ('"ok"\n', 200) if ok else ({'error':'Sync to PW failed', 'ok':ok}, 500)

    @app.route('/links/desired', methods=['GET'])
    def get_port_patches():
        return db.get_port_patches()

    @app.route('/links/active', methods=['GET'])
    def get_active_links():
        return pw.get_links()

    def err_resp(e):
        return {
            'type': type(e).__name__,
            'msg': str(e),
            'url': flask.request.url,
            'method': flask.request.method,
            'data': flask.request.data.decode()
        }

    @app.errorhandler(KeyError)
    def handle_exception(err):
        """Generic handler for KeyError as it's probably their fault"""
        response = err_resp(err)
        logging.exception(f'Data error processing request: {response}')
        return response, 400

    @app.errorhandler(ValueError)
    def handle_exception(err):
        """Generic handler for ValueError as it's probably their fault"""
        response = err_resp(err)
        logging.exception(f'Data error processing request: {response}')
        return response, 400

    @app.errorhandler(IndexError)
    def handle_exception(err):
        """Generic handler for IndexError as it's probably their fault"""
        response = err_resp(err)
        logging.exception(f'Data error processing request: {response}')
        return response, 400

    @app.errorhandler(sqlite3.IntegrityError)
    def handle_exception(err):
        """SQL integrity error, more than likely due to non-existant port inidices"""
        response = err_resp(err)
        logging.exception(f'Data error processing request: {response}')
        return response, 400

    @app.errorhandler(sqlite3.Error)
    def handle_exception(err):
        """Unexpected SQL error"""
        response = err_resp(err)
        logging.exception(f'Unexpected Database error: {response}')
        return response, 500

    # A generic fallback for other unhandled exceptions
    @app.errorhandler(Exception)
    def handle_generic_exception(err):
        from werkzeug.exceptions import HTTPException
        if isinstance(err, HTTPException): # Don't interfere with Flask's own HTTP exceptions
            return err
        
        response = err_resp(err)
        logging.exception(f'Unhandled Exception: {response}')
        return flask.jsonify(response), 500


def test_populate(db, pw):
    try:
        update_db_from_pw(db, pw)
        tsrc = [ db.create_src('testsrc1', 'a test source', {'left': 4, 'right': 5}),
                db.create_src('testsrc2', 'a test source', {'left': 5, 'right': 6}) ]
        tdst = [ db.create_dst('testdst1', 'a test destination', {'left': 6, 'right': 7}),
                db.create_dst('testdst2', 'a test destination', {'left': 8, 'right': 9})]
        db.patch(tsrc[0], tdst[1])
        db.patch(tsrc[0], tdst[1])
        db.patch(tsrc[1], tdst[0])
        return db.create_matrix('testmatrix1', f'this is a test at {time.time()}', 0, tsrc, tdst)
    except Exception as e:
        logging.exception(f"{type(e).__name__} Exception populating test data: {e}")

    update_pw_from_db(db, pw)

    with db.connect() as c:
        r=c.execute("SELECT matrix FROM matrix WHERE name == 'testmatrix1'")
        return r.fetchone()[0]
    


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.debug(f"Let's Goooo")
    db = Db()
    app = flask.Flask(__name__)
    CORS(app)
    pw = PW()
    api(app, db, pw)

    # mtx = test_populate(db, pw)
    # print(db.get_matrix_lines(mtx))
    update_db_from_pw(db, pw)
    update_pw_from_db(db, pw)
    print(db.get_port_patches())

    app.run(host='0.0.0.0', debug=True)
