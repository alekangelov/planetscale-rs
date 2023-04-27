use axum::{
    body::HttpBody,
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Extension, Json, Router,
};
use axum_auth::AuthBasic;
use dotenvy::dotenv;
use mysql::{prelude::Queryable, Conn, Opts, Pool, PooledConn};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    any::Any,
    collections::HashMap,
    env,
    sync::{Arc, RwLock},
};
use tower::ServiceBuilder;
use tower_http::{add_extension::AddExtensionLayer, cors::CorsLayer, trace::TraceLayer};
use uuid::{uuid, Uuid};

#[derive(Default, Debug, Serialize, Deserialize)]
struct Config {
    pub connection_url: String,
    pub username: String,
    pub password: String,
    pub port: u32,
}

#[derive(Default, Debug, Serialize, Deserialize)]
struct Field {
    pub name: String,
    pub _type: String,
    pub table: Option<String>,

    pub database: Option<String>,
    pub orgTable: Option<String>,
    pub orgName: Option<String>,

    pub columnLength: Option<u32>,
    pub charset: Option<u32>,
    pub flags: Option<u32>,
    pub columnType: Option<String>,
}
#[derive(Default, Debug, Serialize, Deserialize)]
struct Row {
    pub lengths: Vec<String>,
    pub values: Option<String>,
}

#[derive(Default, Debug, Serialize, Deserialize)]
struct ResultRes {
    pub rowsAffected: Option<String>,
    pub insertId: Option<String>,
    pub fields: Option<Vec<Field>>,
    pub rows: Option<Vec<Row>>,
}
#[derive(Default, Debug, Serialize, Deserialize)]
struct Error {
    pub message: String,
    pub code: u32,
}
#[derive(Default, Debug, Serialize, Deserialize)]
struct ResponseBody {
    pub session: Uuid,
    pub result: Option<ResultRes>,
    pub error: Option<Error>,
    pub timing: Option<u32>,
}

impl ResponseBody {
    fn from_error(error: Error, session: Uuid) -> Self {
        Self {
            error: Some(error),
            result: None,
            session,
            timing: None,
        }
    }

    fn from_session(session: Uuid) -> Self {
        Self {
            session,
            error: None,
            result: None,
            timing: None,
        }
    }
}

struct AppState {
    pub config: Config,
}

type SharedState = Arc<RwLock<AppState>>;

#[derive(Default, Serialize, Deserialize)]
struct RequestBody {
    pub query: Option<String>,
    pub session: Option<Uuid>,
}

impl RequestBody {
    fn default() -> Self {
        Self {
            query: Some("".to_string()),
            session: Some(Uuid::new_v4()),
        }
    }
}

#[axum_macros::debug_handler]
async fn health(
    State(state): State<SharedState>,
    maybe_body: Option<Json<RequestBody>>,
) -> Json<ResponseBody> {
    let Json(body) = match maybe_body {
        Some(x) => x,
        None => Json(RequestBody::default()),
    };
    Json(ResponseBody {
        session: body.session.unwrap(),
        error: None,
        result: None,
        timing: None,
    })
}
async fn execute(
    State(state): State<SharedState>,
    AuthBasic((username, password)): AuthBasic,
    Extension(pool): Extension<Pool>,
    Json(body): Json<RequestBody>,
) -> Json<ResponseBody> {
    let session = match body.session {
        Some(s) => s,
        None => Uuid::new_v4(),
    };
    let password = match password {
        Some(p) => p,
        None => "".to_string(),
    };
    if username != state.read().unwrap().config.username
        || password != state.read().unwrap().config.password
    {
        return Json(ResponseBody::from_error(
            Error {
                message: "Invalid credentials".to_string(),
                code: 401,
            },
            session,
        ));
    }
    let mut conn = pool.get_conn().unwrap();
    let query = body.query.unwrap_or("".to_string());
    let res: Vec<String> = match conn.query(query) {
        Ok(e) => e.to_vec(),
        Err(e) => {
            let arr: Vec<String> = Vec::new();
            arr
        }
    };
    println!("{:?}", res);
    Json(ResponseBody {
        session,
        result: Some(ResultRes {
            fields: None,
            insertId: None,
            rows: None,
            rowsAffected: None,
        }),
        timing: None,
        error: None,
    })
}

async fn session(
    State(state): State<SharedState>,
    AuthBasic((username, password)): AuthBasic,
) -> Json<ResponseBody> {
    let session = Uuid::new_v4();
    let password = match password {
        Some(p) => p,
        None => "".to_string(),
    };
    if username != state.read().unwrap().config.username
        || password != state.read().unwrap().config.password
    {
        return Json(ResponseBody::from_error(
            Error {
                message: "Invalid credentials".to_string(),
                code: 401,
            },
            session,
        ));
    }

    Json(ResponseBody::from_session(session))
}

async fn app(state: AppState, pool: Pool) -> anyhow::Result<Router> {
    let middleware = ServiceBuilder::new()
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .layer(AddExtensionLayer::new(pool))
        .into_inner();
    let router = Router::new()
        .route("/", get(|| async { Json(json!({"status": "ok"})) }))
        .route("/health", post(health))
        .route("/psdb.v1alpha1.Database/Execute", post(execute))
        .route("/psdb.v1alpha1.Database/CreateSession", post(session))
        .layer(middleware)
        .with_state(Arc::new(RwLock::new(state)));

    Ok(router)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let config = Config {
        connection_url: env::var("DATABASE_URL").unwrap(),
        username: env::var("PS_USERNAME").unwrap(),
        password: env::var("PS_PASSWORD").unwrap(),
        port: env::var("PORT")
            .unwrap_or("3000".to_string())
            .parse::<u32>()
            .unwrap(),
    };
    println!("{:?}", config);
    let pool = Pool::new(Opts::from_url(&config.connection_url).unwrap()).unwrap();
    let app_state = AppState { config };

    let mut url = "0.0.0.0:".to_string();
    url.push_str(&app_state.config.port.to_string());
    println!("Listening on {}", url);

    axum::Server::bind(&url.parse().unwrap())
        .serve(app(app_state, pool).await?.into_make_service())
        .await?;
    Ok(())
}
