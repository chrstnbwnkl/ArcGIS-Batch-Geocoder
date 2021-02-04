import json

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
            querystring += " LIMIT 5"
        
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

            if self._exists(row):
                filtered_locations.append(row)
            
        return filtered_locations
            


    def _exists(self, row):
        querystring_template = f"""
        SELECT exists (SELECT 1 FROM user_locations WHERE location->>'locationId'::varchar = {row[1]} LIMIT 1);
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

    def geocode(self, locations):
        """Do the geocoding!

        Parameters
        ----------

        Returns
        -------
        """
            

class UserLocation:
    def __init__(self, loc_str, geom, score, loc_type, loc_id):
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