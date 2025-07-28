import psycopg2

class PGDatabase:
    def __init__(self, host, database, user, password):
        self.conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        self.cursor = self.conn.cursor()

    def post(self, query, values=None):
        try:
            if values:
                self.cursor.execute(query, values)
            else:
                self.cursor.execute(query)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e

    def __del__(self):
        self.cursor.close()
        self.conn.close()