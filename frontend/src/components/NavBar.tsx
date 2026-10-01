"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { HealthBadge } from "./HealthBadge";

const LINKS = [
  { href: "/", label: "Overview" },
  { href: "/games", label: "Top Games" },
  { href: "/analytics", label: "Analytics" },
  { href: "/realtime", label: "Realtime" },
];

export function NavBar() {
  const pathname = usePathname();
  return (
    <header className="sticky top-0 z-10 border-b border-slate-200 bg-white/90 backdrop-blur">
      <div className="mx-auto flex max-w-7xl flex-wrap items-center justify-between gap-3 px-4 py-3">
        <div className="flex items-center gap-6">
          <span className="text-base font-semibold">Steam Analytics</span>
          <nav className="flex gap-1">
            {LINKS.map(({ href, label }) => {
              const active = href === "/" ? pathname === "/" : pathname.startsWith(href);
              return (
                <Link
                  key={href}
                  href={href}
                  className={`rounded-md px-3 py-1.5 text-sm font-medium ${
                    active ? "bg-slate-900 text-white" : "text-slate-600 hover:bg-slate-100"
                  }`}
                >
                  {label}
                </Link>
              );
            })}
          </nav>
        </div>
        <HealthBadge />
      </div>
    </header>
  );
}