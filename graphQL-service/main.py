from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter
from schema import schema

app = FastAPI(title="IoT GraphQL Service - Scenario B")

graphql_app = GraphQLRouter(schema)

app.include_router(graphql_app, prefix="/graphql")

@app.get("/")
def root():
    return {"message": "GraphQL servis je aktivan na /graphql"}