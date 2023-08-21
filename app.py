import pandas as pd
from sqlalchemy import create_engine, func
from flask import Flask, jsonify
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from matplotlib import style
style.use('fivethirtyeight')
from datetime import datetime

# Create an engine to connect to the SQLite database
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# Reflect the database tables
Base = automap_base()
Base.prepare(engine, reflect=True)  # Reflect the tables

# Assign the Measurement and Station classes
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create a session with autoload_with parameter
session = Session(engine)

# Create a Flask app
app = Flask(__name__)

# Define routes
@app.route("/")
def home():
    """List all available routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/&lt;start&gt;<br/>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt;"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    """Return the last 12 months of precipitation data."""
    # Calculate the date one year before the most recent date
    most_recent_date = session.query(func.max(Measurement.date)).scalar()
    most_recent_date = pd.to_datetime(most_recent_date).date()

    one_year_ago = most_recent_date - pd.DateOffset(years=1)
    one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')

    # Query precipitation data
    prcp_data = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date >= one_year_ago_str).all()

    # Convert data to a dictionary
    prcp_dict = {date: prcp for date, prcp in prcp_data}
    
    return jsonify(prcp_dict)

@app.route("/api/v1.0/stations")
def stations():
    """Return a list of stations."""
    station_list = session.query(Station.station).all()

    # Convert the SQLAlchemy Row objects to a list of dictionaries
    stations_data = [{'station': station[0]} for station in station_list]

    return jsonify(stations_data)

@app.route("/api/v1.0/tobs")
def tobs():
    """Return temperature observations for the most active station for the previous year."""
    # Query the most active station
    most_active_station = session.query(Measurement.station).\
        group_by(Measurement.station).\
        order_by(func.count(Measurement.station).desc()).\
        first()[0]

    # Calculate the date one year before the most recent date
    most_recent_date = session.query(func.max(Measurement.date)).scalar()
    most_recent_date = pd.to_datetime(most_recent_date)
    one_year_ago = most_recent_date - pd.DateOffset(years=1)
    
    # Convert the one_year_ago date to string format 'YYYY-MM-DD'
    one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')

    # Query temperature data for the most active station
    temperature_data = session.query(Measurement.date, Measurement.tobs).\
        filter(Measurement.station == most_active_station, Measurement.date >= one_year_ago_str).all()

    # Convert the query results to a list of dictionaries
    temperature_list = [{'date': date, 'tobs': tobs} for date, tobs in temperature_data]

    return jsonify(temperature_list)

@app.route("/api/v1.0/<start>")
def temp_stats_start(start):
    """Return temperature statistics for a specified start date."""
    # Convert input start date to a datetime object
    start_date = pd.to_datetime(start).date()

    # Query temperature statistics for the specified date
    temp_stats = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date == start_date).\
        all()

    # Convert results to a dictionary
    stats_dict = {
        "date": start_date,
        "TMIN": temp_stats[0][0],
        "TAVG": temp_stats[0][1],
        "TMAX": temp_stats[0][2]
    }

    return jsonify(stats_dict)

@app.route("/api/v1.0/<start>/<end>")
def temp_stats(start, end=None):
    """Return temperature statistics for a specified date range."""
    # Convert input dates to datetime objects
    start_date = datetime.strptime(start, '%Y-%m-%d')
    
    if end:
        end_date = datetime.strptime(end, '%Y-%m-%d')
    else:
        end_date = session.query(func.max(Measurement.date)).scalar()
        end_date = datetime.strptime(str(end_date), '%Y-%m-%d')  # Convert to datetime object
    
    # Query temperature statistics
    temp_stats = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date, Measurement.date <= end_date).\
        all()

    # Convert results to a dictionary
    stats_dict = {
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": end_date.strftime('%Y-%m-%d'),
        "TMIN": temp_stats[0][0],
        "TAVG": temp_stats[0][1],
        "TMAX": temp_stats[0][2]
    }

    return jsonify(stats_dict)


# Run the app
if __name__ == "__main__":
    app.run(debug=True)