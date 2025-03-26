import { createSignal } from "solid-js";

interface Props {
    isDisabled: () => boolean;
    handleSubmit: (prompt: string) => void;
}

export default function TextBox(props: Props) {
  const [prompt, setPrompt] = createSignal<string>("");

  const disableSubmit = () => {
    return (props.isDisabled() || prompt() === "");
  };

  function handleSubmit() {
    props.handleSubmit(prompt());
    setPrompt("");
  };

  return (
    <div class="fixed bottom-10 flex justify-center items-center px-2 max-w-170 w-170 bg-green-50 shadow-md p-0 rounded-3xl border-1 border-slate-300">
      <input
        name="prompt"
        type="text"
        disabled={props.isDisabled()}
        value={prompt()}
        autofocus
        onInput={(e) => setPrompt(e.target.value)}
        onKeyDown={(e: KeyboardEvent) => {
            if (e.key === "Enter" && prompt())
                handleSubmit();
        }}
        placeholder="Ask anything"
        class="w-full h-12 px-4 py-2 rounded-3xl outline-0 ring-0 placeholder:text-slate-400 text-black"
      />

      <button
        onClick={handleSubmit}
        disabled={disableSubmit()}
        class="flex justify-center items-center text-green-50 min-w-[35px] min-h-[35px] max-w-[35px] max-h-[35px] p-0 disabled:bg-slate-400 not-disabled:bg-slate-900 rounded-full self-center duration-200 transition-colors hover:not-disabled:bg-slate-700">
        <svg
          xmlns="http://www.w3.org/2000/svg"
          height="24px"
          viewBox="0 -960 960 960"
          width="20px"
          fill="currentColor">
          <path d="M440-160v-487L216-423l-56-57 320-320 320 320-56 57-224-224v487h-80Z" />
        </svg>
      </button>
    </div>
  );
}
