import os
import mysql.connector
import pandas as pd
from IPython.display import display
from mysql.connector import Error
from simple_term_menu import TerminalMenu


def clear_screen():
    os.system("clear")


def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")


def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
    except Error as err:
        print(f"Error: '{err}'")

    return connection


create_transactions_table = """
CREATE TABLE transactions (
    date DATE NOT NULL,
    type VARCHAR(10) NOT NULL,
    amount DECIMAL(19,2) UNSIGNED NOT NULL,
    account VARCHAR(30) NOT NULL,
    fund VARCHAR(30) NOT NULL,
    note VARCHAR(60) NOT NULL,
    flag INT UNSIGNED NOT NULL
);
"""

create_assets_table = """
CREATE TABLE assets (
    date DATE NOT NULL,
    type VARCHAR(10) NOT NULL,
    amount DECIMAL(19,2) UNSIGNED NOT NULL,
    account VARCHAR(30) NOT NULL,
    fund VARCHAR(30) NOT NULL,
    note VARCHAR(60) NOT NULL,
    flag INT UNSIGNED NOT NULL
);
"""

create_liabilities_table = """
CREATE TABLE liabilities (
    date DATE NOT NULL,
    type VARCHAR(10) NOT NULL,
    amount DECIMAL(19,2) UNSIGNED NOT NULL,
    account VARCHAR(30) NOT NULL,
    fund VARCHAR(30) NOT NULL,
    note VARCHAR(60) NOT NULL,
    flag INT UNSIGNED NOT NULL
);
"""


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        for _ in cursor.execute(query, multi=True):
            pass
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")


def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        results = []
        cursor.execute(query)
        result = cursor.fetchall()
        for item in result:
            results.append(item)
        
        return results
    except Error as err:
        print(f"Error: '{err}'")


def get_transaction_data():
    entries = []
    date = input("Enter the transaction date: ")
    entries.append(date)
    type = input("Enter the transaction type: ")
    entries.append(type)
    amount = input("Enter the transaction amount: ")
    entries.append(amount)
    account = input("Enter the transaction account: ")
    entries.append(account)
    fund = input("Enter the transaction fund: ")
    entries.append(fund)
    note = input("Enter the transaction note: ")
    entries.append(note)
    flag = input("Enter the transaction flag: ")
    entries.append(flag)
    return entries


def create_summary_stats_query():
    start_date = input("Enter the start date: ")
    end_date = input("Enter the end date: ")
    query = f"""
    SELECT * FROM transactions 
    WHERE (date BETWEEN '{start_date}' AND '{end_date}')
    """
    return query


def interpret_summary_stats(transactions, assets, liabilities):
    columns = ["Date", "Type", "Amount", "Account", "Fund", "Note", "Flag"]

    transactions_list = []
    for transaction in transactions:
        transaction = list(transaction)
        transactions_list.append(transaction)
   
    transactions_df = pd.DataFrame(transactions_list, columns=columns)
    
    assets_list = []
    for asset in assets:
        asset = list(asset)
        assets_list.append(asset)

    assets_df = pd.DataFrame(assets_list, columns=columns)

    liabilities_list = []
    for liability in liabilities:
        liability = list(liability)
        liabilities_list.append(liability)

    liabilities_df = pd.DataFrame(liabilities_list, columns=columns)


    display(transactions_df)
    display(assets_df)
    display(liabilities_df)

    return transactions_df, assets_df, liabilities_df


def export_summary_stats(transactions_df, assets_df, liabilities_df):
    if(os.path.isfile('/Users/zackarybarnes/desktop/summarystats.csv')):
        transactions_df.to_csv('/Users/zackarybarnes/desktop/summarystats.csv', mode = 'a')
    else:
        transactions_df.to_csv('/Users/zackarybarnes/desktop/summarystats.csv')


    if(os.path.isfile('/Users/zackarybarnes/desktop/summarystats.csv')):
        assets_df.to_csv('/Users/zackarybarnes/desktop/summarystats.csv', mode = 'a')
    else:
        assets_df.to_csv('/Users/zackarybarnes/desktop/summarystats.csv')


    if(os.path.isfile('/Users/zackarybarnes/desktop/summarystats.csv')):
        liabilities_df.to_csv('/Users/zackarybarnes/desktop/summarystats.csv', mode = 'a')
    else:
        liabilities_df.to_csv('/Users/zackarybarnes/desktop/summarystats.csv')


def create_transaction_query(entries):
    query = ["INSERT INTO transactions VALUES ("]
    for idx, entry in enumerate(entries):
        if idx < (len(entries) - 1):
            query.append(f"'{entry}', ")
        else:
            query.append(f"'{entry}');")
    
    string_query = "".join(query)
    return string_query


def create_update_tables_query(entries):
    transaction_type = entries[1]
    transaction_amount = entries[2]
    transaction_account = entries[3]
    transaction_fund = entries[4]
    new_query = []
    if transaction_type == "income":
        return f"""
        UPDATE assets
        SET amount = amount + {transaction_amount}
        WHERE account = '{transaction_account}'
        AND fund = '{transaction_fund}';
        UPDATE liabilities
        SET amount = amount - {transaction_amount}
        WHERE account = '{transaction_account}'
        AND fund = '{transaction_fund}';
        """
    elif transaction_type == "expense":
        return f"""
        UPDATE assets
        SET amount = amount - {transaction_amount}
        WHERE account = '{transaction_account}'
        AND fund = '{transaction_fund}';
        UPDATE liabilities
        SET amount = amount + {transaction_amount}
        WHERE account = '{transaction_account}'
        AND fund = '{transaction_fund}';
        """
    elif transaction_type == "asset":
        new_query.append(f"INSERT INTO assets VALUES (")
    elif transaction_type == "liability":
        new_query.append(f"INSERT INTO liabilities VALUES (")
    elif transaction_type == "setasset":
        return f"""
        UPDATE assets
        SET amount = {transaction_amount}
        WHERE account = '{transaction_account}'
        AND fund = '{transaction_fund}';
        """
    elif transaction_type == "setliability":
        return f"""
        UPDATE liabilities
        SET amount = {transaction_amount}
        WHERE account = '{transaction_account}'
        AND fund = '{transaction_fund}';
        """
    else:
        print("Input not valid yet")

    for idx, entry in enumerate(entries):
        if idx < (len(entries) - 1):
            new_query.append(f"'{entry}', ")
        else:
            new_query.append(f"'{entry}');")

    new_query_string = "".join(new_query)
    return new_query_string
    

menu1 = ["[a] Add transaction", "[g] Get summary statistics", "[q] Quit", "[r] Reset tables"]
menu2 = ["[y] Yes", "[n] No"]
menu3 = ["[y] Yes", "[n] No"]
mainMenu = TerminalMenu(menu1, title = "Main Menu")
resetMenu = TerminalMenu(menu2, title = "***ARE YOU SURE YOU WANT TO RESET THE TABLES***")
exportMenu = TerminalMenu(menu3, title = "Would you like to export summary statistics?")

clear_screen()
dbName = input("Hello, please enter your database name: ")
dbPass = input("Please enter your database password: ")
clear_screen()

connection = create_server_connection("localhost", "root", dbPass)
create_database(connection, f"CREATE DATABASE {dbName}")

connection = create_db_connection("localhost", "root", dbPass, dbName)
execute_query(connection, create_transactions_table)
execute_query(connection, create_assets_table)
execute_query(connection, create_liabilities_table)

input("Press Enter to continue: ")
clear_screen()


run = True
while run:
    main_menu_options = mainMenu.show()
    options_choice = menu1[main_menu_options]

    if(options_choice == "[a] Add transaction"):
        entries = get_transaction_data()
        transaction_query = create_transaction_query(entries)
        update_query = create_update_tables_query(entries)
        execute_query(connection, transaction_query)
        execute_query(connection, update_query)
    elif(options_choice == "[g] Get summary statistics"):
        transaction_stats_query = create_summary_stats_query()
        transaction_stats_raw_data = read_query(connection, transaction_stats_query)
        asset_stats_raw_data = read_query(connection, "SELECT * FROM assets")
        liability_stats_raw_data = read_query(connection, "SELECT * FROM liabilities")
        transactions_df, assets_df, liabilities_df = interpret_summary_stats(transaction_stats_raw_data, asset_stats_raw_data, liability_stats_raw_data)
        input("Press Enter to continue: ")
        exportOption = exportMenu.show()
        exportChoice = menu3[exportOption]
        if(exportChoice == "[y] Yes"):
            export_summary_stats(transactions_df, assets_df, liabilities_df)
        elif(exportChoice == "[n] No"):
            print(exportChoice)
        else:
            print(exportChoice)
    elif(options_choice == "[q] Quit"):
        quit()
    elif(options_choice == "[r] Reset tables"):
        confirmation = resetMenu.show()
        confirmation_choice = menu2[confirmation]
        if(confirmation_choice == "[y] Yes"):
            execute_query(connection, "DELETE FROM transactions;")
            execute_query(connection, "DELETE FROM assets;")
            execute_query(connection, "DELETE FROM liabilities;")
        elif(confirmation_choice == "[n] No"):
            print(confirmation_choice)
        else:
            print(confirmation_choice)
    else:
        print(options_choice)