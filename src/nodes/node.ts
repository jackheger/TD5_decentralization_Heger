import express from "express";
import { BASE_NODE_PORT } from "../config";
import { NodeState, Value } from "../types";
import { delay } from "../utils";

async function node(nodeId, N, F, initialValue, isFaulty, nodesAreReady, setNodeIsReady) {
  const server = express();
  server.use(express.json());

  const nodeState = {
    killed: false,
    x: !isFaulty ? initialValue : null,
    decided: !isFaulty ? false : null,
    k: !isFaulty ? 0 : null,
  };

  const proposalRecords = new Map();
  const voteRecords = new Map();

  server.get("/status", (request, response) => {
    response.status(isFaulty ? 500 : 200).send(isFaulty ? "faulty" : "live");
  });

  server.post("/message", async (request, response) => {
    const { k, x, type } = request.body;

    if (!isFaulty && !nodeState.killed) {
      handleMessageType(type, k, x, N, F, proposalRecords, voteRecords, nodeState);
    }

    response.status(200).send("Message acknowledged.");
  });

  server.get("/start", async (request, response) => {
    while (!nodesAreReady()) await delay(5);

    if (!isFaulty) {
      initiateConsensus(N, initialValue, nodeState);
    }

    response.status(200).send("Consensus process initiated.");
  });

  server.get("/stop", async (request, response) => {
    nodeState.killed = true;
    response.status(200).send("Node stopped.");
  });

  server.get("/getState", (request, response) => {
    response.status(200).send(nodeState);
  });

  return server.listen(BASE_NODE_PORT + nodeId, () => {
    console.log(`Node server ${nodeId} running at port ${BASE_NODE_PORT + nodeId}`);
    setNodeIsReady(nodeId);
  });
}

function handleMessageType(type, k, x, N, F, proposalRecords, voteRecords, nodeState) {
  switch (type) {
    case "proposal":
      updateProposals(proposalRecords, k, x, N, F, nodeState);
      break;
    case "vote":
      processVotes(voteRecords, k, x, N, F, nodeState);
      break;
    default:
      console.log("Unknown message type received.");
  }
}

function updateProposals(proposals, k, x, N, F, state) {
  if (!proposals.has(k)) proposals.set(k, []);
  proposals.get(k).push(x);

  if (proposals.get(k).length >= N - F) {
    const consensusValue = resolveConsensusValue(proposals.get(k));
    broadcastMessage(N, { k, x: consensusValue, type: "vote" });
  }
}

function processVotes(votes, k, x, N, F, state) {
  if (!votes.has(k)) votes.set(k, []);
  votes.get(k).push(x);

  if (votes.get(k).length >= N - F) {
    const [zeros, ones] = tallyVotes(votes.get(k));

    if (zeros >= F + 1 || ones >= F + 1) {
      finalizeDecision(zeros, ones, state);
    } else {
      const consensusValue = resolveConsensusValue(votes.get(k));
      state.k = state.k ? state.k + 1 : 1;
      broadcastMessage(N, { k: state.k, x: consensusValue, type: "proposal" });
    }
  }
}

function initiateConsensus(N, initialValue, state) {
  state.k = 1;
  state.x = initialValue;
  state.decided = false;
  broadcastMessage(N, { k: state.k, x: state.x, type: "proposal" });
}

function broadcastMessage(N, message) {
  for (let i = 0; i < N; i++) {
    sendMessage(i, message);
  }
}

function resolveConsensusValue(values) {
  const tally = [0, 0];
  values.forEach(value => {
    if (value !== '?') tally[value]++;
  });
  return tally[0] === tally[1] ? Math.round(Math.random()) : tally[0] > tally[1] ? 0 : 1;
}

function finalizeDecision(zeros, ones, state) {
  state.x = zeros > ones ? 
  0 : 1;
  state.decided = true;
}
  
function tallyVotes(votes) {
  return votes.reduce(
    ([zeros, ones], vote) => {
      if (vote !== '?') {
        vote === 0 ? zeros++ : ones++;
      }
      return [zeros, ones];
    },
    [0, 0]
  );
}

function sendMessage(nodeId, { k, x, type }) {
  fetch(`http://localhost:${BASE_NODE_PORT + nodeId}/message`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ k, x, type }),
  }).catch((err) => console.error(`Error sending message: ${err}`));
}
