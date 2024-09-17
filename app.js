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
  PREFIX dbpedia: <http://dbpedia.org/ontology/>
  PREFIX omgeving: <https://data.vlaanderen.be/ns/omgevingsvergunning#>
  SELECT ?created 
  {
    GRAPH ?g {
      ?uri a dbpedia:Case ;
           omgeving:ingangsdatum ?created .
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

  // takes triples of Case and one-deep relationships.
  const queryCases = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX mu:   <http://mu.semte.ch/vocabularies/core/>
    PREFIX dbpedia: <http://dbpedia.org/ontology/>
    PREFIX omgeving: <https://data.vlaanderen.be/ns/omgevingsvergunning#>
    SELECT ?uri {
      GRAPH ?g {
       select ?uri ?created {
        ?uri a dbpedia:Case ;
            omgeving:ingangsdatum ?created .
        FILTER ( ?created > ${sparqlEscapeDateTime(date)})    
      } ORDER BY DESC(?created) LIMIT 10 
    }
  `;
  // do this query to the external sparql endpoint (?)
  const connectionOptions = {
    sparqlEndpoint: "http://the.custom.endpoint/sparql",
    mayRetry: true,
  };
  result = await querySudo(queryCases, {}, connectionOptions);
  bindings = result.results.bindings;
  if (bindings.length === 0) {
    return; // no new cases
  }  
  const urisCases = bindings.map(b => b.uri.value);
  // query all the triples that should be transfered.
  // omgeving:zaakhandeling is handled separately to only copy to the org (municipality) it is part of
  const queryInfo = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX mu:   <http://mu.semte.ch/vocabularies/core/>
    PREFIX dbpedia: <http://dbpedia.org/ontology/>
    PREFIX omgeving: <https://data.vlaanderen.be/ns/omgevingsvergunning#>
    SELECT DISTINCT ?orgId ?s ?p ?o {
      GRAPH ?g {
       ?uri omgeving:zaakhandeling ?submission . 
       ?submission omgeving:Rechtshandeling.verantwoordelijke/mu:uuid ?orgId .
       {
        ?uri ?p ?o .
        ?s ?p ?o .
        FILTER( p != omgeving:zaakhandeling )
       }
       UNION {
        ?uri dct:subject ?s .
        ?s ?p ?o . 
       } UNION {
        ?submission ?p ?s .
        ?s ?p ?o . 
       }
        UNION {
         ?uri dct:subject/omgeving:locatie ?s .
        ?s ?p ?o . 
       }
        UNION {
         ?uri dct:subject/omgeving:Activiteit.tijdsbestek ?s .
        ?s ?p ?o . 
       } UNION {
         ?submission omgeving:aanvrager ?s .
        ?s ?p ?o . 
       } UNION {
        ?submission omgeving:aanvrager ?s .
        ?s ?p ?o . 
       }
       VALUES ?uri { ${urisCases.map(uri => sparqlEscapeUri(uri)).join(" ")} }
    }
  `;
  // new aanvragen, add to database
  // todo: Will these include the neccesary mu:uuid's ?
  result = await querySudo(queryInfo, {}, connectionOptions);
  bindings = result.results.bindings;
  if (bindings.length === 0) {
    return; // no info?
  }
  const triplesToAdd = collapsArrayOfObjects(
    bindings.map((b) => ({
      orgId: b.orgId.value,
      triple: `${b.s.value} ${b.p.value} ${b.o.value}`,
    })), "orgId", "triple", "triples"
  );

  const queryAddAanvragen = `
  INSERT DATA {
    ${triplesToAdd.map(({orgId, triples}) => {
      `
      GRAPH <${PREFIX_ORGANIZATION_GRAPH}${orgId}> {
      ${triples.join(" .\n")}
    }`
    })}
  }
`;
  await updateSudo(queryAddAanvragen);

  // add the link between cases and submission where allowed.
  const querySubmissionOfCase = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX mu:   <http://mu.semte.ch/vocabularies/core/>
    PREFIX dbpedia: <http://dbpedia.org/ontology/>
    PREFIX omgeving: <https://data.vlaanderen.be/ns/omgevingsvergunning#>
    SELECT DISTINCT ?orgId ?uri ?submission {
      GRAPH ?g {
      ?uri omgeving:zaakhandeling ?submission . 
      ?submission omgeving:Rechtshandeling.verantwoordelijke/mu:uuid ?orgId .
      VALUES ?uri { ${urisCases.map(uri => sparqlEscapeUri(uri)).join(" ")} }
    }
  `;
  result = await querySudo(querySubmissionOfCase, {}, connectionOptions);
  bindings = result.results.bindings;
  if (bindings.length === 0) {
    return; // no info?
  }

  const submissionsToAdd = collapsArrayOfObjects(
    bindings.map((b) => ({
      orgId: b.orgId.value,
      triple: `${b.uri.value} omgeving:zaakhandeling ${b.submission.value}`,
    })), "orgId", "triple", "triples"
  );

  const submissionsToAddQuery = `
  INSERT DATA {
    ${submissionsToAdd.map(({orgId, triples}) => {
      `
      GRAPH <${PREFIX_ORGANIZATION_GRAPH}${orgId}> {
      ${triples.join(" .\n")}
    }`
    })}
  }
`;
  await updateSudo(submissionsToAddQuery);
  
}

app.use(errorHandler);
