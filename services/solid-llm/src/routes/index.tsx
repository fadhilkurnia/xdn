import { createAsync } from "@solidjs/router";
import TextBox from "~/components/TextBox";
import { For, createSignal, createEffect, Show } from "solid-js";
import { Portal } from "solid-js/web";
import { getPrompts, postPrompt } from "~/lib/db";


export default function Home() {
  // const [data, setData] = createSignal<Chat[]>();
  const [disablePrompt, setDisablePrompt] = createSignal(false);
  const data = createAsync(() => getPrompts());

  async function handleSubmit(prompt: string) {
    try {
      setDisablePrompt(true);
      const response = await postPrompt(prompt);

      if (!response) throw new Error("Failed to generate prompt response");
      else setDisablePrompt(false);
    } catch (err) {
      console.error(err);
    }
  }

  let containerRef: HTMLDivElement | undefined;
  createEffect(() => {
    if (containerRef && data()) {
      containerRef.scrollIntoView({ block: "end" });
    }
  });

  return (
    <main
      ref={containerRef}
      class="min-h-screen w-screen bg-gradient-to-br from-green-50 to-green-100 flex flex-col items-center px-2 py-25 overflow-y-scroll z-10">
      <div class="max-w-170 flex w-170 flex-col gap-4">
        <For each={data()}>
          {(chat) => (
            <>
              <div class="bg-slate-600 text-green-50 p-2 rounded-lg self-end">
                {chat.prompt}
              </div>
              <div class="text-slate-900 p-2">{chat.response}</div>
            </>
          )}
        </For>
      </div>

      <TextBox isDisabled={disablePrompt} handleSubmit={handleSubmit} />

      <Show when={disablePrompt()}>
        <Portal>
          <div class="fixed top-0 left-0 flex justify-center items-center h-screen w-screen z-100 bg-transparent backdrop-blur-xs">
            <svg
              class="animate-spin h-8 w-8 text-blue-500"
              fill="currentColor"
              viewBox="0 0 24 24"
              xmlns="http://www.w3.org/2000/svg">
              <path
                d="M12,4a8,8,0,0,1,7.89,6.7A1.53,1.53,0,0,0,21.38,12h0a1.5,1.5,0,0,0,1.48-1.75,11,11,0,0,0-21.72,0A1.5,1.5,0,0,0,2.62,12h0a1.53,1.53,0,0,0,1.49-1.3A8,8,0,0,1,12,4Z"
              />
            </svg>
          </div>
        </Portal>
      </Show>
    </main>
  );
}