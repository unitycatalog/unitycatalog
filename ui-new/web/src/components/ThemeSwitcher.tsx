import { Monitor, Moon, Sun } from "lucide-react";
import { useTheme, type ColorMode } from "@/lib/theme";
import { cn } from "@/lib/utils";

const OPTIONS: { mode: ColorMode; label: string; Icon: typeof Sun }[] = [
  { mode: "light", label: "Light", Icon: Sun },
  { mode: "dark", label: "Dark", Icon: Moon },
  { mode: "auto", label: "System", Icon: Monitor },
];

// Segmented light / dark / auto control. Rendered on the dark header bar, so it
// uses white-on-transparent styling rather than theme tokens.
export default function ThemeSwitcher() {
  const { mode, setMode } = useTheme();
  return (
    <div className="inline-flex items-center rounded-md border border-white/30 bg-white/10 p-0.5 backdrop-blur">
      {OPTIONS.map(({ mode: m, label, Icon }) => (
        <button
          key={m}
          type="button"
          onClick={() => setMode(m)}
          title={label}
          aria-label={`${label} theme`}
          aria-pressed={mode === m}
          className={cn(
            "inline-flex h-7 w-7 items-center justify-center rounded transition",
            mode === m ? "bg-white/90 text-neutral-900 shadow-sm" : "text-white/90 hover:bg-white/20",
          )}
        >
          <Icon className="h-4 w-4" />
        </button>
      ))}
    </div>
  );
}
