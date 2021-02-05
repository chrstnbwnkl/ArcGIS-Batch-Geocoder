from geocoder import Geocoder, UserLocation
import logging
import psycopg2 as db
from dotenv import dotenv_values


def main():
    env = dotenv_values()
    db_creds= {key: env[key] for key in ["dbname", "user", "password", "host", "port"]}
    conn = db.connect(**db_creds)
    iter_conn = db.connect(**db_creds)
    gc = Geocoder(conn, iter_conn, debug=False)

    gc.geocode()
    conn.close()
    iter_conn.close()

if __name__ == "__main__":
    main()