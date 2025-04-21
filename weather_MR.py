from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
from datetime import datetime

class WeatherAnalysis(MRJob):
    
    def mapper_extract_temperatures(self, _, line):
        # Skip header row and empty lines
        if line.startswith('time,tavg,tmin,tmax,prcp') or not line.strip():
            return
            
        try:
            # Parse CSV line
            row = next(csv.reader([line]))
            date_str = row[0]
            tavg = float(row[1]) if row[1] else None
            
            # Only process if we have average temperature
            if tavg is not None:
                # Parse date in DD-MM-YYYY format
                date_obj = datetime.strptime(date_str, '%d-%m-%Y')
                year = date_obj.year
                
                # Emit year and temperature data
                yield year, tavg
                
        except (ValueError, IndexError, TypeError) as e:
            # Skip malformed lines
            self.increment_counter('errors', 'data_errors', 1)
            pass
    
    def reducer_avg_temperatures(self, year, temps):
        # Convert to list and filter out None values
        temps = [t for t in temps if t is not None]
        
        if temps:  # Only yield if we have valid temperatures
            avg_temp = sum(temps) / len(temps)
            yield None, (year, round(avg_temp, 2))
    
    def reducer_find_extremes(self, _, year_temp_pairs):
        years_temps = list(year_temp_pairs)
        
        if not years_temps:
            yield "Result", "No valid temperature data found"
            return
            
        if len(years_temps) == 1:
            year, avg = years_temps[0]
            yield "Single Year Analysis", f"Only data for {year} found with average temperature {avg}°C"
        else:
            # Find hottest and coolest years
            hottest_year = max(years_temps, key=lambda x: x[1])
            coolest_year = min(years_temps, key=lambda x: x[1])
            
            yield "Hottest Year", f"{hottest_year[0]} (Avg: {hottest_year[1]}°C)"
            yield "Coolest Year", f"{coolest_year[0]} (Avg: {coolest_year[1]}°C)"
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract_temperatures,
                   reducer=self.reducer_avg_temperatures),
            MRStep(reducer=self.reducer_find_extremes)
        ]

if __name__ == '__main__':
    WeatherAnalysis.run()