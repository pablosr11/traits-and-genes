const btnUpload = document.getElementById("btnUpload");
const progressTracker = document.getElementById("progressTracker");
const f = <HTMLInputElement>document.getElementById("f");
const resultsBtn = document.getElementById("results");
let resultsLink: string;

function hashThis(s: string) {
  let hash = 5381;
  let i = s.length;
  while (i) hash = (hash * 33) ^ s.charCodeAt(--i);
  return hash >>> 0;
}

function is_valid(theFile: File) {
  return (
    theFile.type != "text/csv" ||
    theFile.size > 30_000_000 ||
    theFile.size < 15_000_000
  );
}

btnUpload.addEventListener("click", () => {
  const reader = new FileReader();
  const theFile = f.files[0];
  const fileName = Math.random() + theFile.name;

  if (is_valid(theFile)) {
    progressTracker.textContent = "Issue with File";
    return;
  }

  // refactor, small testable functions. SINGLE RESPONSABILITY.
  reader.onload = async (event) => {
    const CHUNK_SIZE = 1_000_000; //1mb
    const chunkCount = Math.ceil(
      (<ArrayBuffer>event.target.result).byteLength / CHUNK_SIZE
    );
    const identifier = hashThis(fileName);
    const url = `http://localhost:8080/upload?i=${identifier}`;

    for (let chunkId = 0; chunkId < chunkCount + 1; chunkId++) {
      const filechunk = <ArrayBuffer>(
        event.target.result.slice(
          chunkId * CHUNK_SIZE,
          chunkId * CHUNK_SIZE + CHUNK_SIZE
        )
      );
      if (chunkId === chunkCount) {
        // last block
        await fetch(url + `&q=${theFile.size}`, {
          method: "POST",
          body: filechunk,
        })
          .then((response) => response.json())
          .then((data) => {
            resultsBtn.hidden = false;
            resultsBtn.textContent = "Results Here";
            resultsLink = data["link"];
          })
          .catch((error) => console.log(error));
      } else {
        await fetch(url, {
          method: "POST",
          body: filechunk,
        }).catch((error) => console.log(error));
      }
      // if a chunk fails, whole file becomes corrupt?

      progressTracker.textContent =
        "Upload progress: " + Math.round((chunkId / chunkCount) * 100) + "%";
    }
  };
  reader.readAsArrayBuffer(theFile);
  f.value = "";
});

resultsBtn.addEventListener("click", async (e) => {
  console.log(`Requesting ${resultsLink}`);
  let r = await fetch(resultsLink);
  let data = await r.json();
  if ("link" in data) {
    progressTracker.textContent = "Completed, click to download";
    window.open(data["link"]);
    await fetch(data["link"]);
  } else {
    progressTracker.textContent = "Not yet, try again in 1 minute";
  }
});
