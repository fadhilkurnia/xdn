const BACK_END_API = "";
const main = document.querySelector("main#main-section");
const promptInput = document.querySelector("#prompt-input");
const promptBtn = document.querySelector("#prompt-button");
const loadingBox = document.querySelector("#loading-box");
const chatBox = document.querySelector("#chat-box");

// Handle loadingBox and disable prompt
function setLoading(status) {
    if (status) {
        loadingBox.classList.remove("hidden");
        loadingBox.classList.add("flex");
        promptBtn.setAttribute("disabled", true);
        promptInput.setAttribute("disabled", true);
    } else {
        loadingBox.classList.remove("flex");
        loadingBox.classList.add("hidden");
        promptInput.removeAttribute("disabled");

        if (!input)
            promptBtn.removeAttribute("disabled");
    }
}

// Fetch Prompts
async function getPrompts() {
    const res = await fetch(`${BACK_END_API}/api/prompt`);

    if (!res.ok)
        return console.log("Error: Request rejected.");

    return await res.json();
}

async function postPrompts(prompt) {
    const res = await fetch(`${BACK_END_API}/api/prompt`, {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify({
            prompt: prompt
        }),
    });

    if (!res.ok)
        return console.log("Error: Request rejected.");

    return await res.json();
}

// Handle promptInput, promptBtn, and loadingBox based on fetchPrompt
let input;
promptInput.addEventListener("input", (e) => {
    input = e.target.value;

    if (!input)
        promptBtn.setAttribute("disabled", true);
    else 
        promptBtn.removeAttribute("disabled");
});

promptInput.addEventListener("keydown", async (e) => {
    if (!input || e.key !== "Enter") return;

    addPrompt(input);
    setLoading(true);
    promptBtn.setAttribute("disabled", true);

    try {
        const data = await postPrompts(input);

        if (data) addResponse(data.response);
    } catch (err) {
        console.error(err);
    } finally {
        setLoading(false);
    }

    input = "";
    promptInput.value = "";
    promptInput.focus();
});


// Add Prompt & Response. Also clear
function addPrompt(prompt) {
    const div = document.createElement("div");
    div.classList.add("bg-slate-600", "text-green-50", "p-2", "rounded-lg", "self-end");
    div.textContent = prompt;
    chatBox.appendChild(div);
    main.scrollIntoView({ block: "end" }); 
}

function addResponse(response) {
    const div = document.createElement("div");
    div.classList.add("text-slate-900", "p-2");
    div.textContent = response;
    chatBox.appendChild(div);
    main.scrollIntoView({ block: "end" }); 
}

// OnLoad
async function onLoad() {
    const createStatus = await fetch(`${BACK_END_API}/api/db/create`, { method: "POST" });

    if (createStatus) {
        const data = await getPrompts();

        if (!data || data.rows.length < 1) return;

        data.rows.forEach(item => {
            addPrompt(item.prompt);
            addResponse(item.response);
        });
    }
}

onLoad();
