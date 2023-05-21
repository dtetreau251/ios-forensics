import pandas as pd
import sqlite3
from datetime import datetime


def create_dataframe():
    # create the dataframe
    try:
        conn = connect_to_database()
        query = create_sql_query()
        df = read_sql_query(conn, query)
    except sqlite3.Error as e:
        print(e)
    return df


def connect_to_database():
    # connect to the database
    try:
        conn = sqlite3.connect("./AddressBook.sqlitedb")
    except sqlite3.Error as e:
        print(e)
    return conn


def create_sql_query():
    # create the sql query
    query = "SELECT ABPerson.ROWID, ABPerson.Prefix, ABPerson.First, ABPerson.Middle, ABPerson.Last, ABPerson.Suffix, ABPerson.Birthday, ABPerson.Organization, ABPerson.JobTitle, ABPerson.Note, ABMultiValue.value FROM ABPerson LEFT JOIN ABMultiValue ON ABPerson.ROWID = ABMultiValue.record_id ORDER BY ABPerson.First DESC"
    return query


def read_sql_query(conn, query):
    # read the sql query into a dataframe
    try:
        df = pd.read_sql_query(query, conn)
    except sqlite3.Error as e:
        print(e)
    return df


def combine_rows_by_rowid(df):
    # if there is more than one row with the same ROWID combine the rows into one row and make the values a list
    df = df.groupby("ROWID").agg(
        {
            "Prefix": "first",
            "First": "first",
            "Middle": "first",
            "Last": "first",
            "Suffix": "first",
            "Birthday": "first",
            "Organization": "first",
            "JobTitle": "first",
            "Note": "first",
            "value": lambda x: list(x),
        }
    )
    return df


def make_columns_for_multiple_contact_values(df):
    # iterate through the value column and create new columns for each value up to 5 columns. Name the columns contact1, contact2, etc.
    for i in range(5):
        df["Contact" + str(i + 1)] = df["value"].apply(
            lambda x: x[i] if len(x) > i else ""
        )
    return df


def replace_none_and_nan_values_with_empty_strings(df):
    # replace all the None values with empty strings
    try:
        df = df.fillna("")
        # replace all the nan values with empty strings
        df = df.replace("NaN", "")
    except sqlite3.Error as e:
        print(e)
    return df


def remove_chars_and_spaces_from_phone_numbers(df):
    # check the values in contact1 through contact5 and take out the +, (, ), and - characters and replace them with nothing so that the phone numbers are in a standard format also remove the spaces from the phone numbers.
    try:
        for i in range(5):
            df["Contact" + str(i + 1)] = df["Contact" + str(i + 1)].apply(
                lambda x: x.replace("+", "")
                .replace("(", "")
                .replace(")", "")
                .replace("-", "")
                .replace(" ", "")
                if x != "" and not "@" in x
                else x
            )
    except sqlite3.Error as e:
        print(e)
    return df


def make_phone_numers_standard_format(df):
    # format numbers in contacts 1 through 5 to be in a standard format. If the number is 10 digits long then add a 1 in front. If the number is 7 digits long then add 1251 in front.
    try:
        for i in range(5):
            df["Contact" + str(i + 1)] = df["Contact" + str(i + 1)].apply(
                lambda x: "1" + x
                if len(x) == 10 and x.isnumeric()
                else "1251" + x
                if len(x) == 7 and x.isnumeric()
                else x
            )
    except sqlite3.Error as e:
        print(e)
    return df


def combine_name_columns_and_add_space(df):
    # create name column and add a space between the Prefix, First, Middle, Last, and Suffix columns if they are not empty
    try:
        df["Name"] = (
            df["Prefix"].apply(lambda x: x + " " if x != "" else "")
            + df["First"].apply(lambda x: x + " " if x != "" else "")
            + df["Middle"].apply(lambda x: x + " " if x != "" else "")
            + df["Last"].apply(lambda x: x + " " if x != "" else "")
            + df["Suffix"].apply(lambda x: x + " " if x != "" else "")
        )
    except sqlite3.Error as e:
        print(e)
    return df


def drop_unused_name_columns(df):
    # delete the Prefix, First, Middle, Last, and Suffix columns
    try:
        df = df.drop(["Prefix", "First", "Middle", "Last", "Suffix"], axis=1)
    except sqlite3.Error as e:
        print(e)
    return df


def covert_birthday_to_float(df):
    # convert the Birthday column to a float
    try:
        df["Birthday"] = df["Birthday"].apply(lambda x: float(x) if x != "" else x)
    except sqlite3.Error as e:
        print(e)
    return df


def convert_birthday_to_date(df):
    # convert the Birthday column from Cocoa time to a date
    try:
        df["Birthday"] = df["Birthday"].apply(
            lambda x: datetime.fromtimestamp(x + 978307200).strftime("%m/%d/%Y")
            if x != ""
            else x
        )
    except sqlite3.Error as e:
        print(e)
    return df


def order_columns(df):
    # order the columns by Name, Birthday, Organization, JobTitle, Note, and then contact1 through contact10
    try:
        df = df[
            [
                "Name",
                "Birthday",
                "Organization",
                "JobTitle",
                "Note",
                "Contact1",
                "Contact2",
                "Contact3",
                "Contact4",
                "Contact5",
            ]
        ]
    except sqlite3.Error as e:
        print(e)
    return df


def sort_by_name(df):
    # sort the dataframe by name
    try:
        df = df.sort_values(by=["Name"])
    except sqlite3.Error as e:
        print(e)
    return df


def rename_job_title_column(df):
    # rename the JobTitle column to Job Title
    try:
        df = df.rename(columns={"JobTitle": "Job Title"})
    except sqlite3.Error as e:
        print(e)


def export_to_excel(df):
    # export to speadsheet
    try:
        df.to_excel("contact-list.xlsx", index=False)
    except sqlite3.Error as e:
        print(e)


def close_connection_to_database(conn):
    # close the connection to the database
    try:
        conn.close()
    except sqlite3.Error as e:
        print(e)


def export_to_csv(df):
    # export to csv
    try:
        df.to_csv("contact-list.csv", index=False)
    except sqlite3.Error as e:
        print(e)


def main():
    # main function
    df = create_dataframe()
    df = combine_rows_by_rowid(df)
    df = make_columns_for_multiple_contact_values(df)
    df = replace_none_and_nan_values_with_empty_strings(df)
    df = remove_chars_and_spaces_from_phone_numbers(df)
    df = make_phone_numers_standard_format(df)
    df = combine_name_columns_and_add_space(df)
    df = drop_unused_name_columns(df)
    df = covert_birthday_to_float(df)
    df = convert_birthday_to_date(df)
    df = order_columns(df)
    df = sort_by_name(df)
    rename_job_title_column(df)
    export_to_excel(df)
    export_to_csv(df)


main()
