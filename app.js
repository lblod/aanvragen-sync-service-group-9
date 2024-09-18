import { app, errorHandler, sparqlEscapeUri, sparqlEscapeDateTime, sparqlEscapeString, query } from "mu";
import { CronJob } from "cron";
import { querySudo, updateSudo } from "@lblod/mu-auth-sudo";
import { collapsArrayOfObjects, joinAndEnd } from "./utils-javascript";

const PREFIX_ORGANIZATION_GRAPH = "http://mu.semte.ch/graphs/organizations/"
const ENDPOINT_LOKET = process.env.ENDPOINT_LOKET || "https://loket-sparql.hackathon-9.s.redhost.be/sparql";
const MOCK_GRAPH = "http://mu.semte.ch/graphs/mock-loket";
const ALWAYS_SYNC = process.env.ALWAYS_SYNC === 'true'? true : false;
const AUTO_SYNC = true;
const CRON_PATTERN = process.env.CRON_PATTERN || "* * * * *"

app.get("/hello", function (req, res) {
  res.send("Hello mu-javascript-template");
});

app.get("/start-sync", function (req, res) {
  startSync();
  res.send("started sync");
});

const syncJob = new CronJob("*/5 * * * *", async function () {
  startSync();
});

if(AUTO_SYNC) {
  syncJob.start();
}

function createTriple(bindings) {
  let triple = `${sparqlEscapeUri(bindings.s.value)} ${sparqlEscapeUri(bindings.p.value)} `
  if(bindings.o.type === 'uri') {
    triple += `${sparqlEscapeUri(bindings.o.value)}`;
  } else if (bindings.o.type === 'literal') {
    triple += `${sparqlEscapeString(bindings.o.value)}`;
  } else if (bindings.o.type === 'typed-literal') {
    triple += `"${bindings.o.value}"^^<${bindings.o.datatype}>`;
  } else {
    console.warn("not supported triple");
    return "";
  }
  return triple;
}

function createConnectionOptionsLoket() {
  if (ENDPOINT_LOKET === "") {
    return {};
  } else {
    return {
      sparqlEndpoint: ENDPOINT_LOKET,
      mayRetry: true,
    };
  }
}
async function startSync() {
  // get the latest aanvraag existing in database, based on dct:created
  // search in any graph (= in any `http://mu.semte.ch/graphs/organizations/` graph)
  const queryString = `
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX dbpedia: <http://dbpedia.org/ontology/>
  PREFIX omgeving: <https://data.vlaanderen.be/ns/omgevingsvergunning#>
  SELECT ?created WHERE
  {
    GRAPH ?g {
      ?uri a <http://dbpedia.org/resource/Case> ;
           omgeving:zaakhandeling/omgeving:ingangsdatum ?created .
    }
  } ORDER BY DESC(?created) LIMIT 1
  `;
  let result = await querySudo(queryString);
  let bindings = result.results.bindings;
  let date;
  if(ALWAYS_SYNC) {
    date = "2023-01-01T00:00:00Z";
  }
  else if (bindings.length > 0) {
    date = result.results.bindings[0].created.value;
  } else {
    date = "2023-01-01T00:00:00Z";
  }

  // takes triples of Case and one-deep relationships.
  const queryCases = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX mu:   <http://mu.semte.ch/vocabularies/core/>
    PREFIX dbpedia: <http://dbpedia.org/ontology/>
    PREFIX omgeving: <https://data.vlaanderen.be/ns/omgevingsvergunning#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    select distinct ?uri WHERE {
      GRAPH ${ENDPOINT_LOKET === "" ? `<${MOCK_GRAPH}>` : "?g"} {
        ?uri a <http://dbpedia.org/resource/Case> ;
            omgeving:zaakhandeling/omgeving:ingangsdatum ?created .
        FILTER ( ?created > ${sparqlEscapeDateTime(date)})    
      }
    } ORDER BY DESC(?created) ${ALWAYS_SYNC? "": "LIMIT 10"}
  `;
  // do this query to the external sparql endpoint (?)
  result = await querySudo(queryCases, {}, createConnectionOptionsLoket());
  bindings = result.results.bindings;
  if (bindings.length === 0) {
    return; // no new cases
  }
  const urisCases = bindings.map((b) => b.uri.value);

  // query all the triples that should be transfered.
  // omgeving:zaakhandeling is handled separately to only copy to the org (municipality) it is part of
  const queryInfo = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX mu:   <http://mu.semte.ch/vocabularies/core/>
    PREFIX dbpedia: <http://dbpedia.org/ontology/>
    PREFIX omgeving: <https://data.vlaanderen.be/ns/omgevingsvergunning#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT DISTINCT ?org ?s ?p ?o WHERE {
      GRAPH ${ENDPOINT_LOKET === "" ? `<${MOCK_GRAPH}>` : "?g"} {
        ?uri omgeving:zaakhandeling ?submission . 
        ?submission omgeving:Rechtshandeling.verantwoordelijke ?org .
        {
          ?uri ?p ?o .
          ?s ?p ?o .
          FILTER( ?p != omgeving:zaakhandeling )
        }
        UNION {
          ?uri dct:subject ?s .
          ?s ?p ?o . 
        } UNION {
          ?submission ?p ?o .
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
        }
        VALUES ?uri { ${urisCases.map((uri) => sparqlEscapeUri(uri)).join(" ")} }
      }
    }
  `;
  // new aanvragen, add to database
  result = await querySudo(queryInfo, {}, createConnectionOptionsLoket());
  bindings = result.results.bindings;
  if (bindings.length === 0) {
    return; // no info?
  }
  const triplesToAdd = collapsArrayOfObjects(
    bindings.map((b) => ({
      org: b.org.value,
      triple: createTriple(b),
    })),
    "org",
    "triple",
    "triples"
  );
  await addTriplesByOrg(triplesToAdd);

  // add the link between cases and submission where allowed.
  const querySubmissionOfCase = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX mu:   <http://mu.semte.ch/vocabularies/core/>
    PREFIX dbpedia: <http://dbpedia.org/ontology/>
    PREFIX omgeving: <https://data.vlaanderen.be/ns/omgevingsvergunning#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT DISTINCT ?org ?uri ?submission WHERE {
      GRAPH ${ENDPOINT_LOKET === "" ? `<${MOCK_GRAPH}>` : "?g"} {
      ?uri omgeving:zaakhandeling ?submission . 
      ?submission omgeving:Rechtshandeling.verantwoordelijke ?org .
      VALUES ?uri { ${urisCases.map((uri) => sparqlEscapeUri(uri)).join(" ")} }
    }
  }
  `;
  result = await querySudo(querySubmissionOfCase, {}, createConnectionOptionsLoket());
  bindings = result.results.bindings;
  if (bindings.length === 0) {
    return; // no info?
  }

  const submissionsToAdd = collapsArrayOfObjects(
    bindings.map((b) => ({
      org: b.org.value,
      triple: `<${b.uri.value}> omgeving:zaakhandeling <${b.submission.value}>`,
    })),
    "org",
    "triple",
    "triples"
  );

  await addTriplesByOrg(submissionsToAdd);




  // add organization info where needed
  const queryOrgInfo = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX mu:   <http://mu.semte.ch/vocabularies/core/>
    PREFIX dbpedia: <http://dbpedia.org/ontology/>
    PREFIX omgeving: <https://data.vlaanderen.be/ns/omgevingsvergunning#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT DISTINCT ?org (?aanvrager as ?s) ?p ?o WHERE {
      GRAPH ${ENDPOINT_LOKET === "" ? `<${MOCK_GRAPH}>` : "?g"} {
        ?uri omgeving:zaakhandeling ?submission . 
        ?submission omgeving:Rechtshandeling.verantwoordelijke ?org .
        ?submission omgeving:aanvrager ?aanvrager .
      }
      GRAPH ${ENDPOINT_LOKET === "" ? `<${MOCK_GRAPH}>` : "?g2"} {
        ?aanvrager ?p ?o .
      }
      VALUES ?uri { ${urisCases.map((uri) => sparqlEscapeUri(uri)).join(" ")} }
    }
  `;
  result = await querySudo(queryOrgInfo, {}, createConnectionOptionsLoket());
  bindings = result.results.bindings;
  if (bindings.length === 0) {
    return; // no info?
  }

  const orgInfoToAdd = collapsArrayOfObjects(
    bindings.map((b) => ({
      org: b.org.value,
      triple: createTriple(b),
    })),
    "org",
    "triple",
    "triples"
  );
  await addTriplesByOrg(orgInfoToAdd);
}

async function addTriplesByOrg(orgAndTriples) {
  const addInfoQuery = `
  PREFIX omgeving: <https://data.vlaanderen.be/ns/omgevingsvergunning#>
  PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
  INSERT DATA {
    ${orgAndTriples.map(({ org, triples }) => {
      return `GRAPH <${PREFIX_ORGANIZATION_GRAPH}${org.split('/').slice(-1)[0]}> {
      ${joinAndEnd(triples," .\n")}
      }`
    }).join(" ")}
  }
`;
  await updateSudo(addInfoQuery);
}

app.use(errorHandler);