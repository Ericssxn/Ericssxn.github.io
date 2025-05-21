import pymysql
import os

DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 3306, 
    'user': 'eric',
    'password': 'eric',
    'database': 'mib_browser_er'
}

def get_db_connection():
    try:
        conn = pymysql.connect(**DB_CONFIG)
        print("Conexión a la base de datos exitosa.")
        return conn
    except pymysql.MySQLError as e:
        print(f"Error de conexión a la base de datos: {e}")
        raise

file_path = r'C:\Users\Eric\Desktop\mib_browser\oids.txt'

def insert_oids():
    conn = None
    cursor = None
    inserted_count = 0

    try:
        if not os.path.exists(file_path):
            print(f"Error: Archivo no encontrado en la ruta especificada: {file_path}")
            return

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            with open(file_path, 'r', encoding='utf-16') as file:
                lines = file.readlines()
            print("Archivo leído con codificación UTF-16.")
        except UnicodeDecodeError:
            print("UTF-16 falló. Intentando con UTF-16 Little Endian.")
            with open(file_path, 'r', encoding='utf-16-le') as file:
                lines = file.readlines()
            print("Archivo leído con codificación UTF-16 LE.")
        except Exception as e:
            print(f"Error al leer el archivo con UTF-16 o UTF-16 LE: {e}")
            raise

        print(f"Intentando procesar {len(lines)} líneas del archivo '{os.path.basename(file_path)}'...")

        for i, line in enumerate(lines):
            original_line = line.rstrip('\r\n')
            processed_line = line.strip()

            if not processed_line:
                continue

            parts = processed_line.split()

            if len(parts) == 2:
                traduccio_oid_val = parts[0].strip().strip('"')
                oid_val = parts[1].strip().strip('"')

                if not oid_val or not traduccio_oid_val:
                    print(f"Advertencia: Saltando línea {i+1} ('{original_line}'): OID o traducción vacía.")
                    continue

                try:
                    cursor.execute("""
                        INSERT INTO oids (oid, traduccio_oid)
                        VALUES (%s, %s)
                    """, (oid_val, traduccio_oid_val))

                    inserted_count += 1

                except pymysql.IntegrityError:
                    print(f"Advertencia: Saltando línea {i+1} ('{original_line}'): OID '{oid_val}' ya existe.")
                    conn.rollback()
                except pymysql.DataError as e:
                    print(f"Error de datos en línea {i+1} ('{original_line}'): {e}")
                    conn.rollback()
                except pymysql.MySQLError as e:
                    print(f"Error de base de datos en línea {i+1} ('{original_line}'): {e}")
                    conn.rollback()
                except Exception as e:
                    print(f"Error inesperado procesando línea {i+1} ('{original_line}'): {e}")
                    conn.rollback()

            else:
                print(f"Advertencia: Saltando línea {i+1} ('{original_line}'): Formato incorrecto. Partes: {parts}")

        conn.commit()
        print("-" * 30)
        print(f"Proceso de inserción finalizado.")
        print(f"Total de líneas leídas del archivo: {len(lines)}")
        print(f"Total de OIDs que intentaron insertarse: {inserted_count}")
        print("Verifica la base de datos para confirmar los datos insertados.")

    except FileNotFoundError:
        print(f"Error: Archivo no encontrado: {file_path}")
    except pymysql.MySQLError as e:
        print(f"Error con MySQL: {e}")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
        if conn:
            conn.rollback()
            print("Se realizó rollback de la transacción debido a un error.")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("-" * 30)

if __name__ == "__main__":
    insert_oids()