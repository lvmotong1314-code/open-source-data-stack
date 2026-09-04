# Data Model

## Business Process

A taxi trip completed in the City of Chicago taxi system.

## Fact Grain

`fct_taxi_trips` has one row per unique taxi trip, identified by `trip_id`.

## Initial Dimensional Model

### Fact

- `fct_taxi_trips`

Measures:
- `trip_seconds`
- `trip_miles`
- `fare`
- `tips`
- `tolls`
- `extras`
- `trip_total`

Identifiers / categorical attributes:
- `trip_id`
- `taxi_id`
- `payment_type`

### Dimensions

- `dim_date`
- `dim_location`
- `dim_company`

`dim_date` will be role-played for trip start and trip end dates.

`dim_location` will be role-played for pickup and dropoff locations.