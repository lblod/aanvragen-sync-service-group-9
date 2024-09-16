import { app, errorHandler, sparqlEscapeUri, sparqlEscapeDateTime } from "mu";
import { CronJob } from "cron";
import { querySudo, updateSudo } from "@lblod/mu-auth-sudo";
import { collapsArrayOfObjects } from "./utils-javascript";

app.get("/hello", function (req, res) {
  res.send("Hello mu-javascript-template");
});

app.get("start-sync", function (req, res) {
  startSync();
  res.send("started sync");
});

const syncJob = new CronJob("*/5 * * * *", async function () {
  startSync();
});
// syncJob.start();

async function startSync() {
  // get the latest aanvraag existing in database, based on dct:created
  // search in any graph (= in any `http://mu.semte.ch/graphs/organizations/` graph)
  const queryString = `
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  SELECT ?created 
  {
    GRAPH ?g {
      ?uri a ext:Aanvraag ;
           dct:created ?created .
    }
  } ORDER BY DESC(?created) LIMIT 1
  `;
  let result = await querySudo(queryString);
  let bindings = result.results.bindings;
  let date;
  if (bindings.length > 0) {
    date = result.results.bindings[0].created.value;
  } else {
    date = "2024-01-01T00:00:00Z";
  }

  // org is refered to "ext:sessionGroup" in GN?
  const queryAanvragen = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX mu:   <http://mu.semte.ch/vocabularies/core/>
    SELECT ?uri ?p ?o ?orgId 
    {
      GRAPH ?g {
       select ?uri {
        ?uri a ext:Aanvraag ;
            dct:created ?created .
        FILTER ( ?created > ${sparqlEscapeDateTime(date)})    
      }  LIMIT 20
        ?uri ?p ?o .
        ?uri ext:Org ?org .
        ?org mu:uuid ?orgId .
      }
    } ORDER BY DESC(?created)
  `;
  // do this query to the external sparql endpoint (?)
  const connectionOptions = {
    sparqlEndpoint: "http://the.custom.endpoint/sparql",
    mayRetry: true,
  };
  result = await querySudo(queryAanvragen, {}, connectionOptions);
  bindings = result.results.bindings;
  if (bindings.length > 0) {
    // new aanvragen, add to database
    const triplesToAdd = collapsArrayOfObjects(
      bindings.map((b) => ({
        org: b.org.value,
        triple: `${b.uri.value} ${b.p.value} ${b.o.value}`,
      })), "org", "triple", "triples"
    );
    const queryAddAanvragen = `
    INSERT DATA {
      ${triplesToAdd.map(({org, triples}) => {
        `
        GRAPH <${PREFIX_ORGANIZATION_GRAPH}${org}> {
        ${triples.join(" .\n")}
      }`
      })}
      
    }
  `;
    await queryUpdate(queryAddAanvragen);
  }
}

app.use(errorHandler);
