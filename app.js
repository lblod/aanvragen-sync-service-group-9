import { app, errorHandler } from "mu";
app.get('/hello', function( req, res ) {
  res.send('Hello mu-javascript-template');
} );

app.use(errorHandler);