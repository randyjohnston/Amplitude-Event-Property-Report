import sqlite3

connection = sqlite3.connect('properties.db')
cursor = connection.cursor()


def connect_to_db():
    cursor = connection.cursor()
    print(
        f'Conntected to SQLite with total changes: {connection.total_changes}')
    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS events")
    cursor.execute("DROP TABLE IF EXISTS event_properties")
    cursor.execute("DROP TABLE IF EXISTS event_properties_values")
    cursor.execute("DROP TABLE IF EXISTS user_properties")
    cursor.execute("DROP TABLE IF EXISTS user_properties_values")

    cursor.execute(
        "CREATE TABLE IF NOT EXISTS events (name TEXT UNIQUE, volume INTEGER, first_seen TEXT, last_seen TEXT)")
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS event_properties (name TEXT, event_name TEXT, volume INTEGER, first_seen TEXT, last_seen TEXT, FOREIGN KEY(event_name) REFERENCES events(name))")
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS event_properties_values (name TEXT UNIQUE, event_name TEXT UNIQUE, value TEXT, volume INTEGER, first_seen TEXT, last_seen TEXT, FOREIGN KEY(name) REFERENCES event_properties(name), FOREIGN KEY(event_name) REFERENCES events(name))")
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS user_properties (name TEXT UNIQUE, event_name TEXT, volume INTEGER, first_seen TEXT, last_seen TEXT, FOREIGN KEY(event_name) REFERENCES events(name))")
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS user_properties_values (name TEXT UNIQUE, event_name TEXT, value TEXT, volume INTEGER, first_seen TEXT, last_seen TEXT, FOREIGN KEY(name) REFERENCES user_properties (name), FOREIGN KEY(event_name) REFERENCES events(name))")
    cursor.close()


def process_event(new_event):
    cursor = connection.cursor()
    print('Event type: ' + new_event['event_type'])
    event_query = 'SELECT volume, first_seen, last_seen FROM events WHERE name = ?'
    event_params = (new_event['event_type'],)
    event_names_cursor = cursor.execute(event_query, event_params)
    event_name = event_names_cursor.fetchone()
    if event_name is None:
        print('Did not find previous event')
        event_insert_params = (
            new_event['event_type'], new_event['event_time'], new_event['event_time'])
        event_insert_query = 'INSERT INTO events VALUES (?, 1, ?, ?)'
        cursor.execute(event_insert_query, event_insert_params)
        connection.commit()
        cursor.close()
    else:
        print('Found previous event: ' + str(event_name[0]))
        new_volume = event_name[0] + 1
        prev_first_seen = event_name[1]
        prev_last_seen = event_name[2]
        new_first_seen = prev_first_seen
        if prev_first_seen > new_event['event_time']:
            new_first_seen = new_event['event_time']
        new_last_seen = prev_last_seen
        if prev_last_seen < new_event['event_time']:
            new_last_seen = new_event['event_time']
        event_update_params = (new_volume, new_first_seen,
                               new_last_seen, new_event['event_type'])
        event_update_query = 'UPDATE events SET volume = ?, first_seen = ?, last_seen = ? WHERE name=?'
        cursor.execute(event_update_query, event_update_params)
        connection.commit()
        cursor.close()


def process_event_property():
    pass


def process_user_property():
    pass


def check_and_write_event_to_db(event):
    print(event)
    process_event(event)
    process_event_property()
    process_user_property()
