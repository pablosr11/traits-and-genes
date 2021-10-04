import express from "express";
import fs from "fs";
import cors from "cors";

const app = express();
const port = 8080; // default port to listen
const HOST = `http://localhost:${port}`;

// specify what origins can send request
// null works for local html file, change when proxy server etc are setup
const corsOptions = {
  origin: `null`,
};

app.use(cors(corsOptions));

// start the Express server
app.listen(port, () => {
  console.log(`server started at ${HOST}`);
});

app.get("/", (req, res) => {
  //load html file directly instead
  res.json({ info: "Wrong site" });
});

app.get("/result/:jobId", (req, res) => {
  // query for job id, return when ready
  res.download("/Users/pablo.sanderson/repos/myheritage/fuploader/TODO");
});

let isready = false;
app.get("/status/:jobId", (req, res) => {
  // check work is ready, if not update field
  if (isready) {
    res.json({ link: `${HOST}/result/${req.params["jobId"]}` });
  } else {
    res.statusCode = 202;
    res.send("Not ready yet");
    isready = true;
  }
});

app.post("/upload", (req, res) => {
  let name = req.query["i"];
  let filename = name + ".csv";

  req.on("data", (chunk) => {
    // add ID to identify each chunk - to detect missing packets
    // this also ties data to the api. ideally we could send it to a DB? to consolidate the chunks later?
    // and use the id to put if together? probs need to add a chunk id too.
    fs.appendFileSync(filename, chunk);
    console.log(`received ${chunk.length}. Storing in ${filename}`);
  });

  // check if final header included, and filesize is equal to it
  // if yes, return link to results download
  if (req.query["q"] && +req.query["q"] === fs.statSync(filename).size) {
    console.log(`Upload completed: ${filename}`);
    res.json({ link: `${HOST}/status/${name}` });
  }
  res.send("OK");
});
