class Country:
    def __init__(self, country_name):
        self.name = country_name
        self.domestic_flight_count = 0
        self.international_flight_count = 0

    def update_flight_count(self, is_domestic_flight):
        if is_domestic_flight:
            self.domestic_flight_count += 1
        else:
            self.international_flight_count += 1

    def get_touple(self):
        return self.name, self.domestic_flight_count, self.international_flight_count


class Countries:
    def __init__(self):
        self.data = {}

    def acc_country(self, country_name, is_domestic_flight):
        # add country to countries dictionary
        if country_name not in self.data:
            self.data[country_name] = Country(country_name)

        # update flights to countries
        self.data[country_name].update_flight_count(is_domestic_flight)

    def get_formated_results(self):
        return [self.data[country].get_touple() for country in sorted(self.data.keys())]
