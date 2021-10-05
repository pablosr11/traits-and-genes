import express from "express";
import fs from "fs";
import cors from "cors";
import axios from "axios";

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
  res.send("Wrong site");
});

app.get("/result/:jobId", (req, res) => {
  // query for job id, return when ready
  res.download("/Users/pablo.sanderson/repos/myheritage/fuploader/TODO");
});

let isready = false;
app.get("/status/:jobId", (req, res) => {
  // check work is ready, if not update field
  // we could check in DB here if all chunks are there.
  if (isready) {
    res.status(200).send(`${HOST}/result/${req.params["jobId"]}`);
  } else {
    res.status(202).send("Not ready yet");
    isready = true;
  }
});

app.post("/upload", async (req, res) => {
  let fileId = req.query["i"] as string;
  var [chunkId, total] = (req.query["d"] as string).split("o");

  req.on("data", (chunk) => {
    // use chunk id to detect missing packets
    // this also ties data to the api. ideally we could send it to a DB? to consolidate the chunks later?
    fs.appendFileSync(fileId + ".csv", chunk);
    console.log(
      `rcv ${chunkId} out of ${total} - ${chunk.length} goes in ${fileId}`
    );
  });

  // check if final header included, and filesize is equal to it
  // if yes, return link to results download
  if (chunkId === total) {
    console.log(`Upload completed: ${fileId}`);
    // trigger background job with ID fileId.
    await axios.post(`http://localhost:8000/candy/${fileId}`);
    res.status(200).send(`${HOST}/status/${fileId}`);
  } else {
    res.status(202).send("OK");
  }
});
