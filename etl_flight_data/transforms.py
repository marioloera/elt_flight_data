import logging


class Airports:
    """
    Airport sample data

    (1,"Goroka Airport","Goroka","Mexico","GKA","AYGA",-6.0,45.1,5282,10,"U","Pacific","airport","OurAirports")
    (2,"Madang Airport","Madang","Sweden","MAG","AYMD",-5.2,15.7,20,10,"U","Pacific","airport","OurAirports")

    output: {
        IATA: Country
    }

    """

    columns = dict(
        airport_id=0,
        name=1,
        city=2,
        country=3,
        iata=4,
        icao=5,
        latitude=6,
        longitude=7,
        altitude=8,
        timezone=9,
        dst=10,
        tz_db_time_zone=11,
        type=12,
        source=13,
    )

    def __init__(self):
        self.data = {}

    @staticmethod
    def _process_row(element):
        """
        Returns aiport iata code and country

        Parameters:
            element (tuple): with the columns dict

        Returns:
            iata (str):
            country (str):
        """
        if not isinstance(element, tuple):
            logging.warning(f"wrong input type: {type(element)}")
            return None
        try:
            country = element[Airports.columns["country"]]
            iata = element[Airports.columns["iata"]]
            return iata, country

        except Exception as ex:
            msg = f"{ex}. row: {element}"
            logging.warning(msg)
            return None

    def process_rows(self, rows):
        """
        Returns a dictionar aiport iata code and country

        Parameters:
            list of tuples with aiprot information

        Returns: dictionary
            {iata: country}
        """
        for row in rows:
            iata, country = Airports()._process_row(row)
            self.data[iata] = country
        return self.data
