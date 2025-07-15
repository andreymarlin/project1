class FDataBase:
    def __init__(self, db):
        self.__db__ = db
        self.__cur = db.cursor()
    
    def find_query(self, query, table_name="cards"):
        results = []
        sql = f"SELECT field22 FROM {table_name} WHERE text LIKE ?"
        search_term = f"%{query}%"
        self.__cur.execute(sql, (search_term,))
        # fetching all the matching rows
        rows = self.__cur.fetchall()
        #Extract the field22 value from each row and add to results lisr
        for row in rows:
            results.append(row[0])
        return results