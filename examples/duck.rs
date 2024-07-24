use duckdb::{params, Connection, Result};

#[derive(Debug)]
struct Person {
    id: i32,
    name: String,
}

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;

    conn.execute_batch(
        r"
          CREATE SEQUENCE seq;
          CREATE TABLE person (
          id   INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq'),
          name VARCHAR);
          INSERT INTO person(name) VALUES ('John');
        ",
    )?;

    let me = Person {
        id: 0,
        name: "Steven".to_string(),
    };
    conn.execute("INSERT INTO person(name) VALUES (?)", params![me.name])?;

    let mut stmt = conn.prepare("SELECT id, name FROM person")?;
    let person_iter = stmt.query_map([], |row| {
        Ok(Person {
            id: row.get(0)?,
            name: row.get(1)?,
        })
    })?;

    for person in person_iter {
        let person = person.unwrap();
        println!("Found person id:{} name:{}", person.id, person.name);
    }
    Ok(())
}
