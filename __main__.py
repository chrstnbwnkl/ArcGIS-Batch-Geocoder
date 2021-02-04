from geocoder import Geocoder, UserLocation
import logging
import psycopg2 as db
from dotenv import dotenv_values


def main():
    env = dotenv_values()
    conn = db.connect(**{key: env[key] for key in ["dbname", "username", "passwort", "host", "port"]})
    iter_conn = db.connect(**{key: env[key] for key in ["dbname", "username", "passwort", "host", "port"]})
    gc = Geocoder(conn, iter_conn)


if __name__ == "__main__":
    main()