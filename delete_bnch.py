from db.schema import get_connection

conn = get_connection()
try:
    conn.execute(
        """
        DELETE
        FROM dim_etfs
        WHERE ticker = 'SHY'
        """
    )

    conn.commit()
    print('borrado exitoso')
except Exception as e:
    print(f'fallo: {e}')