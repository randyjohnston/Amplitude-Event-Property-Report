import sqlite3

connection = sqlite3.connect('properties.db')


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
        "CREATE TABLE IF NOT EXISTS event_properties_values (name TEXT, event_name TEXT, value TEXT, volume INTEGER, first_seen TEXT, last_seen TEXT, FOREIGN KEY(name) REFERENCES event_properties(name), FOREIGN KEY(event_name) REFERENCES events(name))")
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS user_properties (name TEXT, event_name TEXT, volume INTEGER, first_seen TEXT, last_seen TEXT, FOREIGN KEY(event_name) REFERENCES events(name))")
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS user_properties_values (name TEXT, event_name TEXT, value TEXT, volume INTEGER, first_seen TEXT, last_seen TEXT, FOREIGN KEY(name) REFERENCES user_properties (name), FOREIGN KEY(event_name) REFERENCES events(name))")
    cursor.close()


def process_event(new_event):
    cursor = connection.cursor()
    event_query = 'SELECT volume, first_seen, last_seen FROM events WHERE name = ?'
    event_params = (new_event['event_type'],)
    event_names_cursor = cursor.execute(event_query, event_params)
    event_name = event_names_cursor.fetchone()
    if event_name is None:
        event_insert_params = (
            new_event['event_type'], new_event['event_time'], new_event['event_time'])
        event_insert_query = 'INSERT INTO events VALUES (?, 1, ?, ?)'
        cursor.execute(event_insert_query, event_insert_params)
        connection.commit()
        cursor.close()
    else:
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


def process_property(new_property_type, new_event_name, new_event_time, new_property):
    cursor = connection.cursor()
    event_prop_query = f'SELECT volume, first_seen, last_seen FROM {new_property_type}_properties WHERE name = ? AND event_name = ?'
    event_prop_params = (new_property, new_event_name)
    event_prop_names_cursor = cursor.execute(
        event_prop_query, event_prop_params)
    event_prop = event_prop_names_cursor.fetchone()
    if event_prop is None:
        event_prop_insert_params = (
            new_property, new_event_name, new_event_time, new_event_time)
        event_prop_insert_query = f'INSERT INTO {new_property_type}_properties VALUES (?, ?, 1, ?, ?)'
        cursor.execute(event_prop_insert_query, event_prop_insert_params)
        connection.commit()
        cursor.close()
    else:
        new_volume = event_prop[0] + 1
        prev_first_seen = event_prop[1]
        prev_last_seen = event_prop[2]
        new_first_seen = prev_first_seen
        if prev_first_seen > new_event_time:
            new_first_seen = new_event_time
        new_last_seen = prev_last_seen
        if prev_last_seen < new_event_time:
            new_last_seen = new_event_time
        event_prop_update_params = (new_volume, new_first_seen,
                                    new_last_seen, new_property, new_event_name)
        event_prop_update_query = f'UPDATE {new_property_type}_properties SET volume = ?, first_seen = ?, last_seen = ? WHERE name= ? AND event_name = ?'
        cursor.execute(event_prop_update_query, event_prop_update_params)
        connection.commit()
        cursor.close()


def close_connection_to_db():
    print(
        f'Disconnecting from SQLite with total changes: {connection.total_changes}')
    connection.close()


def process_property_value(new_property_type, new_event_name, new_event_time, new_property, new_property_value):
    cursor = connection.cursor()
    # TODO - don't assume new_property_value is a string
    event_prop_values_query = f'SELECT volume, first_seen, last_seen FROM {new_property_type}_properties_values WHERE name = ? AND event_name = ? and value = ?'
    event_prop_values_params = (
        new_property, new_event_name, str(new_property_value))
    event_prop_values_cursor = cursor.execute(
        event_prop_values_query, event_prop_values_params)
    event_prop_value = event_prop_values_cursor.fetchone()
    if event_prop_value is None:
        event_prop_insert_params = (
            new_property, new_event_name, str(new_property_value), new_event_time, new_event_time)
        event_prop_insert_query = f'INSERT INTO {new_property_type}_properties_values VALUES (?, ?, ?, 1, ?, ?)'
        cursor.execute(event_prop_insert_query, event_prop_insert_params)
        connection.commit()
        cursor.close()
    else:
        new_volume = event_prop_value[0] + 1
        prev_first_seen = event_prop_value[1]
        prev_last_seen = event_prop_value[2]
        new_first_seen = prev_first_seen
        if prev_first_seen > new_event_time:
            new_first_seen = new_event_time
        new_last_seen = prev_last_seen
        if prev_last_seen < new_event_time:
            new_last_seen = new_event_time
        event_prop_value_update_params = (new_volume, new_first_seen,
                                          new_last_seen, new_property, new_event_name, str(new_property_value))
        event_prop_value_update_query = f'UPDATE {new_property_type}_properties_values SET volume = ?, first_seen = ?, last_seen = ? WHERE name= ? AND event_name = ? AND value = ?'
        cursor.execute(event_prop_value_update_query,
                       event_prop_value_update_params)
        connection.commit()
        cursor.close()


def check_and_write_event_to_db(event):
    process_event(event)
    event_name = event['event_type']
    event_properties = event['event_properties'].items()
    event_time = event['event_time']
    user_properties = event['user_properties'].items()
    for event_property, event_property_value in event_properties:
        process_property('event', event_name, event_time, event_property)
        process_property_value(
            'event', event_name, event_time, event_property, event_property_value)
    for user_property, user_property_value in user_properties:
        process_property('user', event_name, event_time, user_property)
        process_property_value(
            'user', event_name, event_time, user_property, user_property_value)
    
