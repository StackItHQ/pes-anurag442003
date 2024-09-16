import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import select
import logging

class DatabaseManager:
    def __init__(self, dbname, user, password, host, port):
        try:
            self.conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
            self.cur = self.conn.cursor(cursor_factory=RealDictCursor)
            self.create_tables()
            self.setup_notify()
            logging.info("Database connection established successfully.")
        except psycopg2.OperationalError as e:
            if "database" in str(e) and "does not exist" in str(e):
                logging.warning(f"Database '{dbname}' does not exist. Attempting to create it.")
                self.create_database(dbname, user, password, host, port)
            else:
                logging.error(f"Error connecting to the database: {str(e)}")
                raise

    def create_database(self, dbname, user, password, host, port):
        try:
            conn = psycopg2.connect(dbname='postgres', user=user, password=password, host=host, port=port)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
            cur.close()
            conn.close()
            logging.info(f"Database '{dbname}' created successfully.")
            # Reconnect to the newly created database
            self.conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
            self.cur = self.conn.cursor(cursor_factory=RealDictCursor)
            self.create_tables()
            self.setup_notify()
        except Exception as e:
            logging.error(f"Error creating database: {str(e)}")
            raise

    def create_tables(self):
        try:
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS data_table (
                    id SERIAL PRIMARY KEY,
                    first_name TEXT,
                    last_name TEXT,
                    email TEXT,
                    department TEXT,
                    hire_date TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS sync_status (
                    id SERIAL PRIMARY KEY,
                    last_synced TIMESTAMP
                )
            """)
            self.conn.commit()
            logging.info("Tables created successfully.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error creating tables: {str(e)}")
            raise

    def setup_notify(self):
        try:
            self.cur.execute("""
                CREATE OR REPLACE FUNCTION notify_trigger() RETURNS trigger AS $$
                DECLARE
                BEGIN
                    PERFORM pg_notify('data_change', TG_TABLE_NAME || ',changed,' || NEW.id);
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;

                DROP TRIGGER IF EXISTS data_change_trigger ON data_table;
                CREATE TRIGGER data_change_trigger AFTER INSERT OR UPDATE OR DELETE ON data_table
                FOR EACH ROW EXECUTE FUNCTION notify_trigger();
            """)
            self.conn.commit()
            self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            logging.info("NOTIFY trigger set up successfully.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error setting up NOTIFY trigger: {str(e)}")
            raise

    def listen_for_changes(self, callback):
        self.cur.execute("LISTEN data_change;")
        logging.info("Listening for notifications on channel 'data_change'")
        while True:
            if select.select([self.conn],[],[],5) == ([],[],[]):
                logging.debug("No notification received (timeout)")
            else:
                self.conn.poll()
                while self.conn.notifies:
                    notify = self.conn.notifies.pop(0)
                    logging.info(f"Received notification: {notify.payload}")
                    callback(notify.payload)

    def create(self, table, data):
        columns = data.keys()
        values = [data[column] for column in columns]
        insert_stmt = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING id").format(
            sql.Identifier(table),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )
        try:
            self.cur.execute(insert_stmt, values)
            id_of_new_row = self.cur.fetchone()['id']
            self.conn.commit()
            logging.info(f"Created new row with id: {id_of_new_row}")
            return id_of_new_row
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error creating new row: {str(e)}")
            raise

    def read(self, table, id=None):
        try:
            if id:
                select_stmt = sql.SQL("SELECT * FROM {} WHERE id = %s").format(sql.Identifier(table))
                self.cur.execute(select_stmt, (id,))
                return self.cur.fetchone()
            else:
                select_stmt = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table))
                self.cur.execute(select_stmt)
                return self.cur.fetchall()
        except Exception as e:
            logging.error(f"Error reading from table: {str(e)}")
            raise

    def update(self, table, id, data):
        set_stmt = sql.SQL(", ").join(
            sql.Composed([sql.Identifier(k), sql.SQL(" = "), sql.Placeholder(k)]) for k in data.keys()
        )
        update_stmt = sql.SQL("UPDATE {} SET {} WHERE id = {} RETURNING id").format(
            sql.Identifier(table),
            set_stmt,
            sql.Literal(id)
        )
        try:
            self.cur.execute(update_stmt, data)
            updated_id = self.cur.fetchone()['id']
            self.conn.commit()
            logging.info(f"Updated row with id: {updated_id}")
            return updated_id
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error updating row: {str(e)}")
            raise

    def delete(self, table, id):
        delete_stmt = sql.SQL("DELETE FROM {} WHERE id = %s RETURNING id").format(sql.Identifier(table))
        try:
            self.cur.execute(delete_stmt, (id,))
            deleted_id = self.cur.fetchone()['id']
            self.conn.commit()
            logging.info(f"Deleted row with id: {deleted_id}")
            return deleted_id
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error deleting row: {str(e)}")
            raise

    def get_last_sync_time(self):
        try:
            self.cur.execute("SELECT last_synced FROM sync_status WHERE id = 1")
            result = self.cur.fetchone()
            return result['last_synced'] if result else None
        except Exception as e:
            logging.error(f"Error getting last sync time: {str(e)}")
            raise

    def update_last_sync_time(self):
        try:
            self.cur.execute("UPDATE sync_status SET last_synced = CURRENT_TIMESTAMP WHERE id = 1")
            self.conn.commit()
            logging.info("Updated last sync time")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error updating last sync time: {str(e)}")
            raise

if __name__ == '__main__':
    db = DatabaseManager(dbname='google_sheets_sync', user='your_username', password='your_password', host='localhost', port='5432')
    
    
    new_id = db.create('data_table', {'column1': 'test1', 'column2': 'test2'})
    print(f"Created new row with id: {new_id}")
    
    data = db.read('data_table', new_id)
    print(f"Read data: {data}")
    
    updated_id = db.update('data_table', new_id, {'column1': 'updated_test1'})
    print(f"Updated row with id: {updated_id}")
    
    deleted_id = db.delete('data_table', new_id)
    print(f"Deleted row with id: {deleted_id}")
    
    
    last_sync = db.get_last_sync_time()
    print(f"Last sync time: {last_sync}")
    
    db.update_last_sync_time()
    print("Updated last sync time")