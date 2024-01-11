from neo4j import GraphDatabase
from neomodel import StructuredNode, IntegerProperty, StringProperty, RelationshipTo, RelationshipFrom, config
from kinopoisk_unofficial.kinopoisk_api_client import KinopoiskApiClient
from kinopoisk_unofficial.request.films.film_request import FilmRequest
from kinopoisk_unofficial.request.staff.staff_request import StaffRequest
from kinopoisk_unofficial.request.staff.person_request import PersonRequest

URI = "#"
AUTH = ("#", "#")
driver4j = GraphDatabase.driver(URI, auth=AUTH)

print(driver4j.verify_connectivity())

KINOPOISK_API_TOKEN = "#"
api_client = KinopoiskApiClient(KINOPOISK_API_TOKEN)

initialFilmID = 942396

def getFilmInfo(filmID): # /api/v2.2/films/{id}
    request = FilmRequest(filmID)
    response = api_client.films.send_film_request(request)
    return response

def getConnectedActors(filmID): # /api/v1/staff
    request = StaffRequest(filmID)
    response = api_client.staff.send_staff_request(request)
    return response


def getActorInfo(actorID): # /api/v1/staff/{id}
    request = PersonRequest(actorID)
    response = api_client.staff.send_person_request(request)
    return response


def createFilmNode(tx, filmInfo):
   query = (
        "MERGE (film:Film {kinopoisk_id: $kinopoisk_id}) "
        "ON CREATE SET film.title = $title"
    )
   tx.run(query, kinopoisk_id=filmInfo.film.kinopoisk_id, title=filmInfo.film.name_ru)


def createActorNode(tx, actorInfo):
    query = (
        "MERGE (actor:Actor {person_id: $person_id}) "
        "ON CREATE SET actor.name = $name"
    )
    tx.run(query, person_id=actorInfo.personId, name=actorInfo.nameRu)


def dbEntityExists(tx, label, property_name, property_value):
    query = f"MATCH (entity:{label} {{{property_name}: $value}}) RETURN COUNT(entity) as count"
    result = tx.run(query, value=property_value).single()
    return result["count"] > 0

we_go_on = True

currentFilmID = initialFilmID

#filmInfo = getFilmInfo(initialFilmID)
films_to_add = set()  # Для отслеживания посещенных фильмов

while we_go_on:
    filmInfo = getFilmInfo(initialFilmID)
    # Создаем узел фильма
    with driver4j.session() as session:
        session.write_transaction(createFilmNode, filmInfo)

    # Получаем актеров и связываем их с фильмом
    connectedActors = getConnectedActors(currentFilmID).items
    for actor in connectedActors:
        actorInfo = getActorInfo(actor.staff_id)

        # Проверяем, существует ли актер в базе данных
        with driver4j.session() as session:
            if not session.read_transaction(dbEntityExists, "Actor", "person_id", actorInfo.personId):
                # Если актера нет, создаем узел актера
                with driver4j.session() as session:
                    session.write_transaction(createActorNode, actorInfo)

        # Создаем связь между фильмом и актером
        with driver4j.session() as session:
            query = (
                "MATCH (film:Film {kinopoisk_id: $film_id}), (actor:Actor {person_id: $person_id}) "
                "MERGE (film)-[:ACTOR]->(actor)"
            )
            session.run(query, film_id=currentFilmID, person_id=actorInfo.personId)

        # Получаем фильмы, в которых участвовал актер, и добавляем их в базу данных
        actorFilms = actorInfo.films
        for film in actorFilms:
            filmInfo = getFilmInfo(film.film_id)
            films_to_add.add(film.film_id)
            # Проверяем, существует ли фильм в базе данных
            with driver4j.session() as session:
                if not session.read_transaction(dbEntityExists, "Film", "kinopoisk_id", filmInfo.film.kinopoisk_id):
                    # Если фильма нет, создаем узел фильма
                    with driver4j.session() as session:
                        session.write_transaction(createFilmNode, filmInfo)

                # Создаем связь между актером и фильмом
                with driver4j.session() as session:
                    query = (
                        "MATCH (actor:Actor {person_id: $person_id}), (film:Film {kinopoisk_id: $kinopoisk_id}) "
                        "MERGE (actor)-[:ACTOR]->(film)"
                    )
                    session.run(query, person_id=actorInfo.personId, kinopoisk_id=filmInfo.film.kinopoisk_id)

    # Получаем следующий фильм для цикла
    currentFilmID = films_to_add.pop()
    try:
        filmInfo = getFilmInfo(currentFilmID)
    except Exception as e:
        print(f"Error fetching film with ID {currentFilmID}: {e}")
        we_go_on = False

