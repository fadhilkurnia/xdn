const BACK_END_API = "";
const main = document.querySelector("main#main-section");
const promptInput = document.querySelector("#prompt-input");
const promptBtn = document.querySelector("#prompt-button");
const loadingBox = document.querySelector("#loading-box");
const chatBox = document.querySelector("#chat-box");
let isLoading = false;

function syncPromptState() {
    const hasInput = promptInput.value.trim().length > 0;
    promptBtn.toggleAttribute("disabled", isLoading || !hasInput);
}

function scrollToLatest() {
    main.scrollTo({ top: main.scrollHeight, behavior: "smooth" });
}

// Handle loadingBox and disable prompt
function setLoading(status) {
    isLoading = status;

    if (status) {
        loadingBox.classList.remove("hidden");
        loadingBox.classList.add("flex");
        promptInput.setAttribute("disabled", true);
    } else {
        loadingBox.classList.remove("flex");
        loadingBox.classList.add("hidden");
        promptInput.removeAttribute("disabled");
    }

    syncPromptState();
}

// Fetch Prompts
async function getPrompts() {
    const res = await fetch(`${BACK_END_API}/api/prompt`);

    if (!res.ok)
        throw new Error("Request rejected.");

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
        throw new Error("Request rejected.");

    return await res.json();
}

// Handle promptInput, promptBtn, and loadingBox based on fetchPrompt
promptInput.addEventListener("input", () => {
    syncPromptState();
});

async function submitPrompt() {
    const prompt = promptInput.value.trim();
    if (!prompt || isLoading) return;

    addPrompt(prompt);
    promptInput.value = "";
    syncPromptState();
    setLoading(true);

    try {
        const data = await postPrompts(prompt);

        if (data?.response)
            addResponse(data.response);
        else
            addResponse("The server did not return a response.");
    } catch (err) {
        console.error(err);
        addResponse("Unable to process the request right now.");
    } finally {
        setLoading(false);
        promptInput.focus();
    }
}

promptInput.addEventListener("keydown", async (e) => {
    if (e.key !== "Enter") return;

    await submitPrompt();
});

promptBtn.addEventListener("click", async () => {
    await submitPrompt();
});

// Add Prompt & Response. Also clear
function addPrompt(prompt) {
    const div = document.createElement("div");
    div.classList.add("bg-slate-600", "text-green-50", "p-2", "rounded-lg", "self-end");
    div.textContent = prompt;
    chatBox.appendChild(div);
    scrollToLatest();
}

function addResponse(response) {
    const div = document.createElement("div");
    div.classList.add("text-slate-900", "p-2");
    div.textContent = response;
    chatBox.appendChild(div);
    scrollToLatest();
}

// OnLoad
async function onLoad() {
    try {
        const data = await getPrompts();

        if (!data || data.rows.length < 1) return;

        data.rows.forEach(item => {
            addPrompt(item.prompt);
            addResponse(item.response);
        });
    } catch (err) {
        console.error(err);
    }
}

onLoad();
syncPromptState();
