  def fetch(self, queries):
    result=[]
    conn, cursor = self.connect()
    try:
      for query in queries:
        cursor.execute(query)
      result = cursor.fetchall()

    except Exception as e:
      print('Exception occured while fetching')
      print(queries)
      print(e)
      raise 
      
    else:
      conn.commit()
      
    finally:
        self.close(conn)
    return result