import datetime
import psycopg2.extras as p
import logging
import pandas as pd

from typing import Any, Dict, List
from utils.db import PostgresConnection, get_postgis_creds


def get_airbnb_data() -> List[Dict[str, Any]]:
    csv_file = 'etl/data/listings.csv'
    try:
        df = pd.read_csv(csv_file)
        df['latitude_longitude'] = "POINT(0, 0)"
        column_types = df.dtypes
        for column in df.columns:
            if pd.api.types.is_float_dtype(column_types[column]):
                df[column] = df[column].fillna(0.0)
            elif pd.api.types.is_string_dtype(column_types[column]):
                df[column] = df[column].fillna('')
            elif pd.api.types.is_datetime64_any_dtype(column_types[column]):
                df[column] = df[column].fillna(pd.NaT)

    except Exception as ex:
        logging.error(f"There was an error with the csv file, {ex}")

    return df.to_dict(orient='records')


def _get_airbnb_insert_query() -> str:
    return '''
    INSERT INTO public.airbnb_listings (
        id,
        listing_url,
        scrape_id,
        last_scraped,
        source,
        name,
        description,
        neighborhood_overview,
        picture_url,
        host_id,
        host_url,
        host_name,
        host_since,
        host_location,
        host_about,
        host_response_time,
        host_response_rate,
        host_acceptance_rate,
        host_is_superhost,
        host_thumbnail_url,
        host_picture_url,
        host_neighbourhood,
        host_listings_count,
        host_total_listings_count,
        host_verifications,
        host_has_profile_pic,
        host_identity_verified,
        neighbourhood,
        neighbourhood_cleansed,
        neighbourhood_group_cleansed,
        latitude,
        longitude,
        property_type,
        room_type,
        accommodates,
        bathrooms,
        bathrooms_text,
        bedrooms,
        beds,
        amenities,
        price,
        minimum_nights,
        maximum_nights,
        minimum_minimum_nights,
        maximum_minimum_nights,
        minimum_maximum_nights,
        maximum_maximum_nights,
        minimum_nights_avg_ntm,
        maximum_nights_avg_ntm,
        calendar_updated,
        has_availability,
        availability_30,
        availability_60,
        availability_90,
        availability_365,
        calendar_last_scraped,
        number_of_reviews,
        number_of_reviews_ltm,
        number_of_reviews_l30d,
        first_review,
        last_review,
        review_scores_rating,
        review_scores_accuracy,
        review_scores_cleanliness,
        review_scores_checkin,
        review_scores_communication,
        review_scores_location,
        review_scores_value,
        license,
        instant_bookable,
        calculated_host_listings_count,
        calculated_host_listings_count_entire_homes,
        calculated_host_listings_count_private_rooms,
        calculated_host_listings_count_shared_rooms,
        reviews_per_month,
        latitude_longitude
    )
    VALUES (
        %(id)s,
        %(listing_url)s,
        %(scrape_id)s,
        %(last_scraped)s,
        %(source)s,
        %(name)s,
        %(description)s,
        %(neighborhood_overview)s,
        %(picture_url)s,
        %(host_id)s,
        %(host_url)s,
        %(host_name)s,
        %(host_since)s,
        %(host_location)s,
        %(host_about)s,
        %(host_response_time)s,
        %(host_response_rate)s,
        %(host_acceptance_rate)s,
        %(host_is_superhost)s,
        %(host_thumbnail_url)s,
        %(host_picture_url)s,
        %(host_neighbourhood)s,
        %(host_listings_count)s,
        %(host_total_listings_count)s,
        %(host_verifications)s,
        %(host_has_profile_pic)s,
        %(host_identity_verified)s,
        %(neighbourhood)s,
        %(neighbourhood_cleansed)s,
        %(neighbourhood_group_cleansed)s,
        %(latitude)s,
        %(longitude)s,
        %(property_type)s,
        %(room_type)s,
        %(accommodates)s,
        %(bathrooms)s,
        %(bathrooms_text)s,
        %(bedrooms)s,
        %(beds)s,
        %(amenities)s,
        %(price)s,
        %(minimum_nights)s,
        %(maximum_nights)s,
        %(minimum_minimum_nights)s,
        %(maximum_minimum_nights)s,
        %(minimum_maximum_nights)s,
        %(maximum_maximum_nights)s,
        %(minimum_nights_avg_ntm)s,
        %(maximum_nights_avg_ntm)s,
        %(calendar_updated)s,
        %(has_availability)s,
        %(availability_30)s,
        %(availability_60)s,
        %(availability_90)s,
        %(availability_365)s,
        %(calendar_last_scraped)s,
        %(number_of_reviews)s,
        %(number_of_reviews_ltm)s,
        %(number_of_reviews_l30d)s,
        %(first_review)s,
        %(last_review)s,
        %(review_scores_rating)s,
        %(review_scores_accuracy)s,
        %(review_scores_cleanliness)s,
        %(review_scores_checkin)s,
        %(review_scores_communication)s,
        %(review_scores_location)s,
        %(review_scores_value)s,
        %(license)s,
        %(instant_bookable)s,
        %(calculated_host_listings_count)s,
        %(calculated_host_listings_count_entire_homes)s,
        %(calculated_host_listings_count_private_rooms)s,
        %(calculated_host_listings_count_shared_rooms)s,
        %(reviews_per_month)s,
        POINT(%(latitude)s, %(longitude)s)
    );
    '''


def run() -> None:
    data = get_airbnb_data()
    # for d in data:
    #     d['update_dt'] = datetime.datetime.now()
    with PostgresConnection(get_postgis_creds()).managed_cursor() as curr:
        p.execute_batch(curr, _get_airbnb_insert_query(), data)


if __name__ == '__main__':
    run()
