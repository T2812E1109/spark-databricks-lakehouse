# Databricks Notebook source

# MAGIC %run ./01-config

import time


class SetupHelper:
    def __init__(self, env):
        self.conf = Config()
        self.landing_zone = f"{self.conf.base_dir_data}/raw"
        self.checkpoint_base = f"{self.conf.base_dir_checkpoint}/checkpoints"
        self.catalog = env
        self.db_name = self.conf.db_name
        self.initialized = False

    def execute_sql(self, sql_command, msg_end):
        """Execute SQL command with a completion message."""
        spark.sql(sql_command)
        print(msg_end if self.initialized else "Database not initialized.", end="")

    def check_initialization(self):
        """Ensure the database is initialized before proceeding."""
        if not self.initialized:
            raise ReferenceError(
                "Application database is not defined. Cannot create table in default database."
            )

    def create_db(self):
        spark.catalog.clearCache()
        self.execute_sql(
            f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.db_name}",
            "Creating the database...",
        )
        spark.sql(f"USE {self.catalog}.{self.db_name}")
        self.initialized = True

    def create_table(self, table_name, schema, partition_by=None):
        """Generalized table creation method."""
        self.check_initialization()
        partition_clause = f"PARTITIONED BY ({partition_by})" if partition_by else ""
        self.execute_sql(
            f"CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.{table_name}({schema}) {partition_clause}",
            f"Creating {table_name} table...",
        )

    def create_view(self, view_name, select_statement):
        """Generalized view creation method."""
        self.check_initialization()
        self.execute_sql(
            f"CREATE OR REPLACE VIEW {self.catalog}.{self.db_name}.{view_name} AS {select_statement}",
            f"Creating {view_name} view...",
        )

    def setup(self):

        start = time.time()
        print("\nStarting setup ...")
        self.create_db()
        self.create_db()
        self.create_registered_users_bz()
        self.create_gym_logins_bz()
        self.create_kafka_multiplex_bz()
        self.create_users()
        self.create_gym_logs()
        self.create_user_profile()
        self.create_heart_rate()
        self.create_workouts()
        self.create_completed_workouts()
        self.create_workout_bpm()
        self.create_user_bins()
        self.create_date_lookup()
        self.create_workout_bpm_summary()
        self.create_gym_summary()

        print(f"Setup completed in {time.time() - start} seconds")

    def create_registered_users_bz(self):
        self.create_table(
            "registered_users_bz",
            "user_id long, device_id long, mac_address string, registration_timestamp double, load_time timestamp, source_file string",
        )

    def create_gym_logins_bz(self):
        self.create_table(
            "gym_logins_bz",
            "mac_address string, gym bigint, login double, logout double, load_time timestamp, source_file string",
        )

    def create_kafka_multiplex_bz(self):
        self.create_table(
            "kafka_multiplex_bz",
            "key string, value string, topic string, partition bigint, offset bigint, timestamp bigint, date date, week_part string, load_time timestamp, source_file string",
        )

    def create_users(self):
        self.create_table(
            "users",
            "user_id bigint, device_id bigint, mac_address string, registration_timestamp timestamp",
        )

    def create_gym_logs(self):
        self.create_table(
            "gym_logs",
            "mac_address string, gym bigint, login timestamp, logout timestamp",
        )

    def create_user_profile(self):
        self.create_table(
            "user_profile",
            "user_id bigint, dob date, sex string, gender string, first_name string, last_name string, street_address string, city string, state string, zip int, updated timestamp",
        )

    def create_heart_rate(self):
        self.create_table(
            "heart_rate",
            "device_id long, time timestamp, heartrate double, valid boolean",
        )

    def create_user_bins(self):
        self.create_table(
            "user_bins",
            "user_id bigint, age string, gender string, city string, state string",
        )

    def create_workouts(self):
        self.create_table(
            "workouts",
            "user_id int, workout_id int, time timestamp, action string, session_id int",
        )

    def create_completed_workouts(self):
        self.create_table(
            "completed_workouts",
            "user_id int, workout_id int, session_id int, start_time timestamp, end_time timestamp",
        )

    def create_workout_bpm(self):
        self.create_table(
            "workout_bpm",
            "user_id int, workout_id int, session_id int, start_time timestamp, end_time timestamp, time timestamp, heartrate double",
        )

    def create_date_lookup(self):
        self.create_table(
            "date_lookup",
            "date date, week int, year int, month int, dayofweek int, dayofmonth int, dayofyear int, week_part string",
        )

    def create_workout_bpm_summary(self):
        self.create_table(
            "workout_bpm_summary",
            "workout_id int, session_id int, user_id bigint, age string, gender string, city string, state string, min_bpm double, avg_bpm double, max_bpm double, num_recordings bigint",
        )

    def create_gym_summary(self):
        self.create_view(
            "gym_summary",
            """SELECT to_date(login::timestamp) date,
                            gym, l.mac_address, workout_id, session_id, 
                            round((logout::long - login::long)/60,2) minutes_in_gym,
                            round((end_time::long - start_time::long)/60,2) minutes_exercising
                            FROM gym_logs l 
                            JOIN (
                            SELECT mac_address, workout_id, session_id, start_time, end_time
                            FROM completed_workouts w INNER JOIN users u ON w.user_id = u.user_id) w
                            ON l.mac_address = w.mac_address 
                            AND w. start_time BETWEEN l.login AND l.logout
                            order by date, gym, l.mac_address, session_id""",
        )

    def assert_table(self, table_name):
        assert (
            spark.sql(f"SHOW TABLES IN {self.catalog}.{self.db_name}")
            .filter(f"isTemporary == false and tableName == '{table_name}'")
            .count()
            == 1
        ), f"The table {table_name} is missing"
        print(f"Found {table_name} table in {self.catalog}.{self.db_name}: Success")

    def validate(self):
        start = int(time.time())
        print(f"\nStarting setup validation ...")
        assert (
            spark.sql(f"SHOW DATABASES IN {self.catalog}")
            .filter(f"databaseName == '{self.db_name}'")
            .count()
            == 1
        ), f"The database '{self.catalog}.{self.db_name}' is missing"
        print(f"Found database {self.catalog}.{self.db_name}: Success")
        self.assert_table("registered_users_bz")
        self.assert_table("gym_logins_bz")
        self.assert_table("kafka_multiplex_bz")
        self.assert_table("users")
        self.assert_table("gym_logs")
        self.assert_table("user_profile")
        self.assert_table("heart_rate")
        self.assert_table("workouts")
        self.assert_table("completed_workouts")
        self.assert_table("workout_bpm")
        self.assert_table("user_bins")
        self.assert_table("date_lookup")
        self.assert_table("workout_bpm_summary")
        self.assert_table("gym_summary")
        print(f"Setup validation completed in {int(time.time()) - start} seconds")

    def cleanup(self):
        """Simplified cleanup method."""
        if self.initialized:
            print(f"Dropping the database {self.catalog}.{self.db_name}...", end="")
            spark.sql(f"DROP DATABASE {self.catalog}.{self.db_name} CASCADE")
            print("Done")
        for path in [self.landing_zone, self.checkpoint_base]:
            print(f"Deleting {path}...", end="")
            dbutils.fs.rm(path, True)
            print("Done")
