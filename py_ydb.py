import ydb;

DropTablesQuery = """
DROP TABLE IF EXISTS series;
DROP TABLE IF EXISTS seasons;
DROP TABLE IF EXISTS episodes;
"""

def drop_tables(pool: ydb.QuerySessionPool):
    print("\nCleaning up existing tables...")
    pool.execute_with_retries(DropTablesQuery)


def create_tables(pool: ydb.QuerySessionPool):
    print("\nCreating table series...")
    pool.execute_with_retries(
        """
        CREATE TABLE `series` (
            `series_id` Int64,
            `title` Utf8,
            `series_info` Utf8,
            `release_date` Date,
            PRIMARY KEY (`series_id`)
        )
        """
    )

    print("\nCreating table seasons...")
    pool.execute_with_retries(
        """
        CREATE TABLE `seasons` (
            `series_id` Int64,
            `season_id` Int64,
            `title` Utf8,
            `first_aired` Date,
            `last_aired` Date,
            PRIMARY KEY (`series_id`, `season_id`)
        )
        """
    )

    print("\nCreating table episodes...")
    pool.execute_with_retries(
        """
        CREATE TABLE `episodes` (
            `series_id` Int64,
            `season_id` Int64,
            `episode_id` Int64,
            `title` Utf8,
            `air_date` Date,
            PRIMARY KEY (`series_id`, `season_id`, `episode_id`)
        )
        """
    )

def run(endpoint, database):
    driver_config = ydb.DriverConfig(
        endpoint, database, credentials=ydb.AccessTokenCredentials("t1.9euelZqTj5KbzsyJxpySm5eeyoueiu3rn*************nMyUjMaPnY-YzJrl8_dbPStA"),
        root_certificates=ydb.load_ydb_root_certificate(),
    )
    with ydb.Driver(driver_config) as driver:
        try:
            driver.wait(timeout=5)
            with ydb.QuerySessionPool(driver) as pool:
                drop_tables(pool)
                create_tables(pool)
        except TimeoutError:
            print("Connect failed to YDB")
            print("Last reported errors by discovery:")
            print(driver.discovery_debug_details())
            exit(1)

run('grpcs://ydb.serverless.yandexcloud.net:2135', '/ru-central1/b1ghhn6*****94oqnrv6/etnvbbq4db6bvelqi1la')

