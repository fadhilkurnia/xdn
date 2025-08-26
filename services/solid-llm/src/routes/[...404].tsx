import { A } from "@solidjs/router";

export default function NotFound() {
  return (
    <main class="w-screen h-screen bg-gradient-to-bl from-red-50 to-red-300 flex flex-col items-center justify-center">
      <h1 class="text-5xl font-medium mb-10">404 Page Not Found</h1>
      <A href="/" class="text-rose-700 font-extrabold hover:underline">
        Go back to Homepage
      </A>
    </main>
  );
}
