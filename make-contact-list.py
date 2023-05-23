import pandas as pd
import sqlite3
from datetime import datetime


def create_sql_query():
    # create the sql query
    query = "SELECT ABPerson.ROWID, ABPerson.Prefix, ABPerson.First, ABPerson.Middle, ABPerson.Last, ABPerson.Suffix, ABPerson.Birthday, ABPerson.Organization, ABPerson.JobTitle, ABPerson.Note, ABMultiValue.value FROM ABPerson LEFT JOIN ABMultiValue ON ABPerson.ROWID = ABMultiValue.record_id ORDER BY ABPerson.First DESC"
    return query


def connect_to_database():
    # connect to the database
    try:
        conn = sqlite3.connect("./AddressBook.sqlitedb")
    except sqlite3.Error as e:
        print(e)
    return conn


def create_dataframe():
    # create the dataframe
    try:
        conn = connect_to_database()
        query = create_sql_query()
        df = read_sql_query(conn, query)
    except sqlite3.Error as e:
        print(e)
    return df


def read_sql_query(conn, query):
    # read the sql query into a dataframe
    try:
        df = pd.read_sql_query(query, conn)
    except sqlite3.Error as e:
        print(e)
    return df


def combine_rows_by_rowid(df):
    # if there is more than one row with the same ROWID combine the rows into one row and make the values a list and replace none values with empty strings
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
            "value": lambda x: list(x) if x is not None else [""],
        }
    )

    return df


def iterate_over_values_and_remove_none_values(df):
    # iterate over the value column lists and remove any None values
    df["value"] = df["value"].apply(lambda x: [i for i in x if i is not None])
    return df


def drop_rows_with_no_values(df):
    # drop rows where the value column is empty
    df = df.dropna(subset=["value"])
    return df


def transform_none_values_to_empty_string_in_data_frame(df):
    # iterate over the dataframe and replace None values with empty strings
    df = df.applymap(lambda x: "" if x is None else x)
    return df


def make_new_phone_columns_with_empty_strings(df):
    # create columns for Phone1 through Phone3
    df["Phone1"] = ""
    df["Phone2"] = ""
    df["Phone3"] = ""
    return df


def make_new_email_columns_with_empty_strings(df):
    # create columns for Email1 through Email2
    df["Email1"] = ""
    df["Email2"] = ""
    return df


def put_emails_in_email_columns_from_lists_in_value_column(df):
    # iterate over the value column lists and take all the values that contain an @ symbol and put them in the appropriate email column in their row
    df["Email1"] = df["value"].apply(
        lambda x: [i for i in x if "@" in i][0]
        if len([i for i in x if "@" in i]) >= 1
        else ""
    )
    df["Email2"] = df["value"].apply(
        lambda x: [i for i in x if "@" in i][1]
        if len([i for i in x if "@" in i]) >= 2
        else ""
    )
    return df


def remove_chars_and_spaces_from_values(df):
    # iterate over the value column lists and remove any characters that are are "(", ")", "-", and " " and replace them with empty strings
    df["value"] = df["value"].apply(
        lambda x: [
            i.replace("(", "")
            .replace(")", "")
            .replace("-", "")
            .replace(" ", "")
            .replace("+", "")
            for i in x
        ]
    )
    return df


def spread_phone_numbers_across_phone_columns(df):
    # iterate over the value column lists and spread the phone numbers across the Phone1, Phone2, and Phone3 columns
    df["Phone1"] = df["value"].apply(
        lambda x: x[0] if len(x) >= 1 and x[0] != "" and x[0].isdigit() else ""
    )
    df["Phone2"] = df["value"].apply(
        lambda x: x[1] if len(x) >= 2 and x[1] != "" and x[1].isdigit() else ""
    )
    df["Phone3"] = df["value"].apply(
        lambda x: x[2] if len(x) >= 3 and x[2] != "" and x[2].isdigit() else ""
    )
    return df


def drop_value_column(df):
    # drop the value column
    df = df.drop(["value"], axis=1)
    return df


def add_area_code_to_7_digit_phone_numbers(df):
    # add the area code to any 7 digit phone numbers
    df["Phone1"] = df["Phone1"].apply(
        lambda x: "251" + x if len(x) == 7 and x != "" else x
    )
    df["Phone2"] = df["Phone2"].apply(
        lambda x: "251" + x if len(x) == 7 and x != "" else x
    )
    df["Phone3"] = df["Phone3"].apply(
        lambda x: "251" + x if len(x) == 7 and x != "" else x
    )
    return df


def add_1_to_phone_numbers_with_10_digits(df):
    # add a 1 to any phone numbers that have 10 digits
    df["Phone1"] = df["Phone1"].apply(
        lambda x: "1" + x if len(x) == 10 and x != "" else x
    )
    df["Phone2"] = df["Phone2"].apply(
        lambda x: "1" + x if len(x) == 10 and x != "" else x
    )
    df["Phone3"] = df["Phone3"].apply(
        lambda x: "1" + x if len(x) == 10 and x != "" else x
    )
    return df


def convert_none_values_in_name_columns_to_empty_string(df):
    # convert None values in the Prefix, First, Middle, Last, and Suffix columns to empty strings
    df["Prefix"] = df["Prefix"].apply(lambda x: "" if x is None else x)
    df["First"] = df["First"].apply(lambda x: "" if x is None else x)
    df["Middle"] = df["Middle"].apply(lambda x: "" if x is None else x)
    df["Last"] = df["Last"].apply(lambda x: "" if x is None else x)
    df["Suffix"] = df["Suffix"].apply(lambda x: "" if x is None else x)
    return df


def combine_name_columns_and_add_space(df):
    # create name column and add a space between the Prefix, First, Middle, Last, and Suffix columns if they are not empty
    df["Name"] = (
        df["Prefix"].apply(lambda x: x + " " if x != "" else "")
        + df["First"].apply(lambda x: x + " " if x != "" else "")
        + df["Middle"].apply(lambda x: x + " " if x != "" else "")
        + df["Last"].apply(lambda x: x + " " if x != "" else "")
        + df["Suffix"].apply(lambda x: x + " " if x != "" else "")
    )
    return df


def drop_unused_name_columns(df):
    # delete the Prefix, First, Middle, Last, and Suffix columns
    df = df.drop(["Prefix", "First", "Middle", "Last", "Suffix"], axis=1)
    return df


def turn_none_values_in_birthdays_column_to_empty_string(df):
    # convert None values in the Birthday column to empty strings
    df["Birthday"] = df["Birthday"].apply(lambda x: "" if x is None else x)
    return df


def covert_birthday_to_float(df):
    # convert the Birthday column to a float
    df["Birthday"] = df["Birthday"].apply(lambda x: float(x) if x != "" else x)
    return df


def convert_birthday_to_date(df):
    # convert the Birthday column from Cocoa time, to Unix, to a date
    df["Birthday"] = df["Birthday"].apply(
        lambda x: datetime.fromtimestamp(x + 978307200).strftime("%m/%d/%Y")
        if x != ""
        else x
    )
    return df


def order_columns(df):
    # order the columns by Name, Birthday, Organization, JobTitle, Note, and then contact1 through contact10
    df = df[
        [
            "Name",
            "Birthday",
            "Organization",
            "JobTitle",
            "Note",
            "Phone1",
            "Phone2",
            "Phone3",
            "Email1",
            "Email2",
        ]
    ]
    return df


def sort_by_name(df):
    # sort the dataframe by name
    df = df.sort_values(by=["Name"])
    return df


def rename_job_title_column(df):
    # rename the JobTitle column to Job Title
    df = df.rename(columns={"JobTitle": "Title"})
    return df


def rename_birthday_column_to_dob(df):
    # rename the Birthday column to DOB
    df = df.rename(columns={"Birthday": "DOB"})
    return df


def drop_rows_with_empty_string_in_name_column(df):
    # drop rows where the Name column is empty
    df = df[df["Name"] != ""]
    return df


def export_to_excel(df):
    # export to speadsheet
    df.to_excel("contact-list.xlsx", index=False)


def export_to_csv(df):
    # export to csv
    df.to_csv("contact-list.csv", index=False)


def close_connection_to_database(conn):
    # close the connection to the database
    try:
        conn.close()
    except sqlite3.Error as e:
        print(e)


def main():
    # main function
    conn = connect_to_database()
    df = create_dataframe()
    df = combine_rows_by_rowid(df)
    df = iterate_over_values_and_remove_none_values(df)
    df = drop_rows_with_no_values(df)
    df = transform_none_values_to_empty_string_in_data_frame(df)
    df = make_new_phone_columns_with_empty_strings(df)
    df = make_new_email_columns_with_empty_strings(df)
    df = put_emails_in_email_columns_from_lists_in_value_column(df)
    df = remove_chars_and_spaces_from_values(df)
    df = spread_phone_numbers_across_phone_columns(df)
    df = drop_value_column(df)
    df = add_area_code_to_7_digit_phone_numbers(df)
    df = add_1_to_phone_numbers_with_10_digits(df)
    df = convert_none_values_in_name_columns_to_empty_string(df)
    df = combine_name_columns_and_add_space(df)
    df = drop_unused_name_columns(df)
    df = turn_none_values_in_birthdays_column_to_empty_string(df)
    df = covert_birthday_to_float(df)
    df = convert_birthday_to_date(df)
    df = order_columns(df)
    df = sort_by_name(df)
    df = rename_job_title_column(df)
    df = rename_birthday_column_to_dob(df)
    df = drop_rows_with_empty_string_in_name_column(df)
    print(df)
    export_to_excel(df)
    export_to_csv(df)
    close_connection_to_database(conn)


main()
