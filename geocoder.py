import json
from requests import get, post
from dotenv import dotenv_values

class Geocoder:
    """Geocoder class that provides batch geocoding functionalities for the ArcGIS world geocoding REST API.
    API docs here: https://developers.arcgis.com/rest/geocode/api-reference/geocoding-geocode-addresses.htm

    Attributes
    ----------


    """
    def __init__(self, conn=None, iter_conn=None, debug=True):
        self._conn = conn
        self._iter_conn = iter_conn
        if conn:
            self._cur = conn.cursor()
        if iter_conn:
            self._iter_cur = iter_conn.cursor()
        self._debug = debug
        self._env = dotenv_values()
    
    @property
    def conn(self):
        return self._conn
    
    @conn.setter
    def conn(self, value):
        self._conn = value
        self._cur = value.cursor()
    
    @property
    def iter_conn(self):
        return self._iter_conn
    
    @iter_conn.setter
    def iter_conn(self, value):
        self._iter_conn = value
        self._iter_cur = value.cursor()

    def _load_addresses(self):
        querystring = """
        SELECT DISTINCT location->'location'->'additionalNames'->>'long'::varchar, (location->'location'->>'locationId')::int
        FROM users 
        WHERE location->'location'->'additionalNames'->>'long'::varchar IS NOT null
        """
        if self._debug == True:
            querystring += " LIMIT 230"
        
        query = self._iter_cur.mogrify(querystring)
        return self._iter_cur.execute(query)

    def _filter_duplicates(self):
        """Checks if the passed list of SingleLine addresses contains addresses that have already been geocoded and
        returns a filtered list of not geocoded addresses.

        Parameters
        ----------
        lst : list
            a list of SingleLine address strings
        """
        filtered_locations = [] 
        self._load_addresses()
        while True:
            row = self._iter_cur.fetchone()

            if row == None:
                break

            if not self._exists(row):
                filtered_locations.append(row)
            
        return filtered_locations
            


    def _exists(self, row):
        querystring_template = f"""
        SELECT exists (SELECT 1 FROM user_locations WHERE id = {row[1]} LIMIT 1);
        """
        query = self._cur.mogrify(querystring_template)
        self._cur.execute(query)

        value = self._cur.fetchone()
        return value[0]
    
    def _make_location_objects(self, locations):
        """Turns a list of locations into a list of dictionaries according to the ArcGIS World Geocoding REST API reuirements.
        
        Parameters
        ----------
        
        locations : list
            list of location strings
            
        Returns
        -------
        
        dict
        """
        return [ {"attributes" : {"OBJECTID": l_id, "SingleLine": l_str}} for l_str, l_id in locations]
    
    def _make_addresses(self, location_objects, max_batch_size):
        """Formats a list of single line addresses into the required format for the ArcGIS World Geocoding REST API.
        
        Parameters
        ----------
        locsaction_objects : dict
            a list of dictionaries of location strings and their OBJECTID
        max_batch_size: int
            the maximum number of addresses to be included in one batch geocoding request to the REST API
        
            
        Returns
        -------
        list
            a list of stringified dictionaries in the format required by the the ArcGIS World Geocoding REST API documentation
        """
        chunked = []
        for loc in self._split_list(location_objects, max_batch_size):
            data = { 'addresses': {}, 'f':'json'}
            records = {"records": loc}
            data["addresses"] = json.dumps(records)
            chunked.append(data)
            
        
        return chunked
    
    def _split_list(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def _get_token(self, user, secret):
        """Generate an ArcGIS token with your ArcGIS user credentials
        
        Parameters
        ----------
        user : str
            the user key generated on your ArcGIS dev portal page
        secret : str
            the user secret generated on your ArcGIS dev portal page
            
        Returns
        -------
        str
            a user token that can be used to access the ArcGIS world geocoding REST API endpoint
        """

        token_creator = """
                        https://www.arcgis.com/sharing/oauth2/token?client_id={}&grant_type=client_credentials&client_secret={}&f=pjson
                        """

        res = get(token_creator.format(user, secret))

        token_data = json.loads(res.content)

        return token_data["access_token"]

    def _make_request(self, payload):

        token = self._get_token(self._env["ArcGIS_user"], self._env["ArcGIS_secret"])
        params = {"token" : token}
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        api_url = "https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/geocodeAddresses"

        r = post(url=api_url, params=params, data=payload, headers=headers)

        return r

    def _insert_user_location(self, ul):
        """Inserts the user location info as a row into the PostgreSQL user locations table
        
        Parameters
        ----------
        ul : UserLocation
        a UserLocation instance
        """
        querystring_template = """
        INSERT INTO user_locations (location, geom, score, type, id) VALUES (%s, ST_SetSRID(ST_MakePoint(%s, %s),4326), %s, %s, %s)
        """

        query = self._cur.mogrify(querystring_template, (ul.loc_str, ul.geom[0], ul.geom[1], ul.score, ul.loc_type, ul.loc_id))
        self._cur.execute(query)
        self._conn.commit()
        return 

    def geocode(self):
        """Do the geocoding!

        Parameters
        ----------

        Returns
        -------
        """      

        locations = self._filter_duplicates()
        location_objects = self._make_location_objects(locations)
        payloads = self._make_addresses(location_objects, 200)

        for payload in payloads:
            res = self._make_request(payload)
            if res.status_code != 200:
                print(f"Something went wrong!")
                break
            else:
                data = json.loads(res.content)
                for geocoded_location in data["locations"]:
                    u_loc = UserLocation()

                    u_loc.loc_str = geocoded_location["attributes"]["LongLabel"]
                    u_loc.geom = [geocoded_location["location"]["x"], geocoded_location["location"]["y"]]
                    u_loc.score = geocoded_location["score"]
                    u_loc.loc_type = geocoded_location["attributes"]["Type"]
                    u_loc.loc_id = geocoded_location["attributes"]["ResultID"]

                    self._insert_user_location(u_loc)
                    print(f"Updated user location {u_loc.loc_str}")         

class UserLocation:
    def __init__(self, loc_str=None, geom=None, score=None, loc_type=None, loc_id=None):
        self._loc_str = loc_str
        self._geom = geom
        self._score = score
        self._loc_type = loc_type
        self._loc_id = loc_id
    
    @property
    def loc_str(self):
        return self._loc_str
    
    @loc_str.setter
    def loc_str(self, value):
        self._loc_str = value
    
    @property
    def geom(self):
        return self._geom
    
    @geom.setter
    def geom(self, value):
        self._geom = value
    
    @property
    def score(self):
        return self._score
    
    @score.setter
    def score(self, value):
        self._score = value
    
    @property
    def loc_type(self):
        return self._loc_type
    
    @loc_type.setter
    def loc_type(self, value):
        self._loc_type = value
    
    @property
    def loc_id(self):
        return self._loc_id
    
    @loc_id.setter
    def loc_id(self, value):
        self._loc_id = value