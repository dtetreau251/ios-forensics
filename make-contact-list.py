import pandas as pd
import sqlite3
from datetime import datetime

# read a sql lite database file and create a dataframe
conn = sqlite3.connect("../AddressBook.sqlitedb")
df = pd.read_sql_query(
    "SELECT ABPerson.ROWID, ABPerson.Prefix, ABPerson.First, ABPerson.Middle, ABPerson.Last, ABPerson.Suffix, ABPerson.Birthday, ABPerson.Organization, ABPerson.JobTitle, ABPerson.Note, ABMultiValue.value FROM ABPerson LEFT JOIN ABMultiValue ON ABPerson.ROWID = ABMultiValue.record_id ORDER BY ABPerson.First DESC",
    conn,
)

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

# iterate through the value column and create new columns for each value up to 10. Name the columns contact1, contact2, etc.
for i in range(10):
    df["Contact" + str(i + 1)] = df["value"].apply(lambda x: x[i] if len(x) > i else "")

# replace all the None values with empty strings
df = df.fillna("")
# replace all the nan values with empty strings
df = df.replace("NaN", "")

# check the values in contact1 through contact10 and take out the +, (, ), and - characters and replace them with nothing so that the phone numbers are in a standard format also remove the spaces from the phone numbers.
for i in range(10):
    df["Contact" + str(i + 1)] = df["Contact" + str(i + 1)].apply(
        lambda x: x.replace("+", "")
        .replace("(", "")
        .replace(")", "")
        .replace("-", "")
        .replace(" ", "")
        if x != "" and not "@" in x
        else x
    )

# format numbers in contacts 1 through 10 to be in a standard format. If the number is 10 digits long then add a 1 in front. If the number is 7 digits long then add 1251 in front.
for i in range(5):
    df["Contact" + str(i + 1)] = df["Contact" + str(i + 1)].apply(
        lambda x: "1" + x
        if len(x) == 10 and x.isnumeric()
        else "1251" + x
        if len(x) == 7 and x.isnumeric()
        else x
    )

# create name column and add a space between the Prefix, First, Middle, Last, and Suffix columns if they are not empty
df["Name"] = (
    df["Prefix"].apply(lambda x: x + " " if x != "" else "")
    + df["First"].apply(lambda x: x + " " if x != "" else "")
    + df["Middle"].apply(lambda x: x + " " if x != "" else "")
    + df["Last"].apply(lambda x: x + " " if x != "" else "")
    + df["Suffix"].apply(lambda x: x + " " if x != "" else "")
)

# convert the Birthday column to a float
df["Birthday"] = df["Birthday"].apply(lambda x: float(x) if x != "" else x)

# convert the Birthday column from Cocoa time to a date
df["Birthday"] = df["Birthday"].apply(
    lambda x: datetime.fromtimestamp(x + 978307200).strftime("%m/%d/%Y")
    if x != ""
    else x
)

# delete the Prefix, First, Middle, Last, and Suffix columns
df = df.drop(["Prefix", "First", "Middle", "Last", "Suffix"], axis=1)

# order the columns by Name, Birthday, Organization, JobTitle, Note, and then contact1 through contact10
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

# rename the JobTitle column to Job Title
df = df.rename(columns={"JobTitle": "Job Title"})

# export to speadsheet
df.to_excel("contact-list.xlsx", index=False)

# close the connection to the database
conn.close()

# export to csv
df.to_csv("contact-list.csv", index=False)
