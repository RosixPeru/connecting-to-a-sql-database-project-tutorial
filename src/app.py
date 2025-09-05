import os
import pandas as pd
from sqlalchemy import create_engine, text, Column, Integer, String, Date, Numeric, ForeignKey, Table
from sqlalchemy.orm import declarative_base, sessionmaker, relationship 
from dotenv import load_dotenv
import psycopg2
from psycopg2 import OperationalError
import os
from sqlalchemy.exc import SQLAlchemyError

# Load environment variables
load_dotenv()

# 1) Connect to the database with SQLAlchemy

def connect():
    global engine
    try:
        connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        print("Starting the connection...")
        engine = create_engine(connection_string, echo=True, isolation_level="AUTOCOMMIT")
        engine.connect()
        print("Connected successfully!")
        return engine
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

engine = connect()

if engine is None:
    exit() 

# 2) Create the tables

# Base declarativa para modelos
Base = declarative_base()

# Definir modelo (tabla)
class Publishers(Base):
    __tablename__ = 'publishers'  # nombre de la tabla en la BD

    publisher_id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)

    def __repr__(self):
        return f"<Publishers(publisher_id={self.publisher_id}, name='{self.name}')>"

# Crear la tabla en la base de datos
try:
    Base.metadata.create_all(engine)
    print("Tabla creada correctamente (si no existía).")
except SQLAlchemyError as e:
    print(f"Error al crear la tabla: {e}")

# Definir modelo (tabla)
class Authors(Base):
    __tablename__ = 'authors'  # nombre de la tabla en la BD

    author_id = Column(Integer, primary_key=True)
    first_name = Column(String(100), nullable=False)
    middle_name = Column(String(50))
    last_name = Column(String(100))

    def __repr__(self):
        return f"<Authors(author_id={self.author_id}, first_name='{self.first_name}')>"

# Crear la tabla en la base de datos
try:
    Base.metadata.create_all(engine)
    print("Tabla creada correctamente (si no existía).")
except SQLAlchemyError as e:
    print(f"Error al crear la tabla: {e}")

# Definir modelo (tabla)
class Books(Base):
    __tablename__ = 'books'  # nombre de la tabla en la BD

    book_id = Column(Integer, primary_key=True)
    title = Column(String(255), nullable=False)
    total_pages = Column(Integer)
    rating = Column(Numeric(4, 2))
    isbn = Column(String(13))
    published_date = Column(Date)
    publisher_id = Column(Integer, ForeignKey('publishers.publisher_id'))
    publisher = relationship("Publishers", backref="books")

    def __repr__(self):
        return f"<Books(book_id={self.book_id}, title='{self.title}')>"

# Crear la tabla en la base de datos
try:
    Base.metadata.create_all(engine)
    print("Tabla creada correctamente (si no existía).")
except SQLAlchemyError as e:
    print(f"Error al crear la tabla: {e}")
    
# Definir modelo (tabla)
class BookAuthor(Base):
    __tablename__ = 'book_authors'  # nombre de la tabla en la BD

    book_id = Column(Integer, ForeignKey('books.book_id', ondelete='CASCADE'), primary_key=True)
    author_id = Column(Integer, ForeignKey('authors.author_id', ondelete='CASCADE'), primary_key=True)

    def __repr__(self):
        return f"<Books(book_id={self.book_id}, title='{self.title}')>"

# Crear la tabla en la base de datos
try:
    Base.metadata.create_all(engine)
    print("Tabla creada correctamente (si no existía).")
except SQLAlchemyError as e:
    print(f"Error al crear la tabla: {e}")
# 3) Insert data

Session = sessionmaker(bind=engine)
session = Session()

#Insertar en publishers
publishers_data = [
    Publishers(publisher_id=1, name='O Reilly Media'),
    Publishers(publisher_id=2, name='A Book Apart'),
    Publishers(publisher_id=3, name='A K PETERS'),
    Publishers(publisher_id=4, name='Academic Press'),
    Publishers(publisher_id=5, name='Addison Wesley'),
    Publishers(publisher_id=6, name='Albert&Sweigart'),
    Publishers(publisher_id=7, name='Alfred A. Knopf')
]

# insertar en authors_data
authors_data = [
    Authors(author_id=1, first_name='Merritt', middle_name=None, last_name='Eric'),
    Authors(author_id=2, first_name='Linda', middle_name=None, last_name='Mui'),
    Authors(author_id=3, first_name='Alecos', middle_name=None, last_name='Papadatos'),
    Authors(author_id=4, first_name='Anthony', middle_name=None, last_name='Molinaro'),
    Authors(author_id=5, first_name='David', middle_name=None, last_name='Cronin'),
    Authors(author_id=6, first_name='Richard', middle_name=None, last_name='Blum'),
    Authors(author_id=7, first_name='Yuval', middle_name='Noah', last_name='Harari'),
    Authors(author_id=8, first_name='Paul', middle_name=None, last_name='Albitz')
]

session.add_all(publishers_data)
session.add_all(authors_data)

try:
    session.commit()
    print("Publishers, authors y books insertados correctamente.")
except Exception as e:
    session.rollback()
    print(f"Error al insertar publishers, authors o books: {e}")

# 4) Use Pandas to read and display a table

# Leer toda la tabla 'books'
df_books = pd.read_sql('SELECT * FROM publishers', con=engine)

# Mostrar el DataFrame
print(df_books)