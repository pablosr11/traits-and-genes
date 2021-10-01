const http = require("http");
const fs = require("fs");
const httpServer = http.createServer();

function hashCode(s) {
  for (var i = 0, h = 0; i < s.length; i++)
    h = (Math.imul(31, h) + s.charCodeAt(i)) | 0;
  return h;
}

httpServer.on("listening", () => console.log("Listening"));
httpServer.on("request", (req, res) => {
  if (req.url === "/") {
    res.end(
      fs.readFileSync(
        "/Users/pablo.sanderson/repos/myheritage/fuploader/index.html"
      )
    );
    return;
  }

  // modify this to wildcard, use that path param as the unique ID for that file.
  if (req.url === "/upload") {
    let name = hashCode(req.headers["user-agent"]) + ".csv";
    req.on("data", (chunk) => {
      // check is the expected chunk, otherwise corrupted file
      validate_chunk(chunk);
      fs.appendFileSync(name, chunk);
      console.log(`received ${chunk.length}`);
    });
    res.end("Done!");
    console.log(`stored in ${name}`);
  }
});

function validate_chunk(chunk) {}
httpServer.listen(8080);
