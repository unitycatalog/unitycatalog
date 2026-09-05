import { useState } from "react";
import type { ReactNode } from "react";
import { PanelLeftClose, PanelLeftOpen, LogOut, User } from "lucide-react";
import { useAuth } from "@/context/auth-context";
import Logo from "@/components/Logo";
import ThemeSwitcher from "@/components/ThemeSwitcher";
import CatalogTree from "@/components/CatalogTree";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { cn } from "@/lib/utils";

export default function Layout({ children }: { children: ReactNode }) {
  const { authEnabled, currentUser, logout } = useAuth();
  const [navOpen, setNavOpen] = useState(true);

  const email = currentUser?.emails?.[0]?.value ?? currentUser?.userName ?? "";
  const displayName = currentUser?.displayName || email || "anonymous";

  return (
    <div className="flex h-full flex-col">
      <header className="sticky top-0 z-30 w-full border-b border-black/30 bg-neutral-900 shadow-sm">
        <div className="flex items-center justify-between px-4 py-2.5">
          <div className="flex items-center gap-3">
            <button
              type="button"
              onClick={() => setNavOpen((v) => !v)}
              aria-expanded={navOpen}
              aria-controls="primary-nav"
              aria-label={navOpen ? "Collapse navigation" : "Expand navigation"}
              title={navOpen ? "Collapse navigation" : "Expand navigation"}
              className="inline-flex h-8 w-8 items-center justify-center rounded-md text-white/90 transition hover:bg-white/15 focus:outline-none focus-visible:ring-2 focus-visible:ring-white/70"
            >
              {navOpen ? <PanelLeftClose className="h-5 w-5" /> : <PanelLeftOpen className="h-5 w-5" />}
            </button>
            <Logo className="h-7 w-auto" title="Unity Catalog" />
          </div>
          <div className="flex items-center gap-3">
            <ThemeSwitcher />
            {authEnabled && (
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <button
                    type="button"
                    className="inline-flex h-8 w-8 items-center justify-center rounded-full bg-white/90 text-neutral-900 transition hover:bg-white focus:outline-none focus-visible:ring-2 focus-visible:ring-white/70"
                    aria-label="Account"
                  >
                    <User className="h-4 w-4" />
                  </button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end" className="w-56">
                  <DropdownMenuLabel>
                    <div className="flex flex-col">
                      <span className="truncate text-sm font-medium">{displayName}</span>
                      {email && <span className="truncate text-xs text-muted-foreground">{email}</span>}
                    </div>
                  </DropdownMenuLabel>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem onClick={() => void logout()}>
                    <LogOut className="mr-2 h-4 w-4" />
                    Log out
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
            )}
          </div>
        </div>
      </header>

      <div className="flex min-h-0 flex-1">
        <aside
          id="primary-nav"
          aria-hidden={!navOpen}
          className={cn(
            "shrink-0 overflow-hidden border-r bg-sidebar transition-[width] duration-300 ease-in-out",
            navOpen ? "w-72 border-border" : "w-0 border-transparent",
          )}
        >
          <div className="flex h-full w-72 flex-col">
            <div className="flex items-center justify-between border-b px-3 py-2.5">
              <span className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                Catalogs
              </span>
              {authEnabled && (
                <Badge variant={currentUser ? "default" : "secondary"}>
                  {currentUser ? "authenticated" : "anonymous"}
                </Badge>
              )}
            </div>
            <div className="min-h-0 flex-1 overflow-auto p-2 text-sm">
              <CatalogTree />
            </div>
            {authEnabled && (
              <div className="border-t p-3">
                <Button variant="outline" className="w-full" onClick={() => void logout()}>
                  <LogOut className="mr-2 h-4 w-4" /> Log out
                </Button>
              </div>
            )}
          </div>
        </aside>
        <main className="min-h-0 flex-1 overflow-auto">{children}</main>
      </div>
    </div>
  );
}
