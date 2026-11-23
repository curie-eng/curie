import type { CSSProperties, ReactNode } from "react";
import { C } from "../tokens";
import { hoverBg } from "../lib/style";

export type ButtonVariant = "primary" | "secondary" | "ghost" | "danger";

const VARIANTS: Record<ButtonVariant, { bg: string; color: string; bd: string; hbg: string }> = {
  primary: { bg: C.brand, color: "#08120c", bd: C.brand, hbg: "#37b87e" },
  secondary: { bg: C.card, color: C.text, bd: C.borderStrong, hbg: C.hover },
  ghost: { bg: "transparent", color: C.text2, bd: "transparent", hbg: C.hover },
  danger: { bg: "transparent", color: C.destructive, bd: C.border, hbg: "rgba(229,77,46,.12)" },
};

export function Button({
  label,
  variant = "secondary",
  size,
  icon,
  full,
  disabled,
  title,
  testId,
  onClick,
}: {
  label: ReactNode;
  variant?: ButtonVariant;
  size?: "sm";
  icon?: string;
  full?: boolean;
  disabled?: boolean;
  title?: string;
  testId?: string;
  onClick?: () => void;
}) {
  const base = VARIANTS[variant];
  const pad = size === "sm" ? "5px 10px" : "8px 14px";
  const style: CSSProperties = {
    background: disabled && variant === "primary" ? C.card : base.bg,
    color: disabled ? C.disabled : base.color,
    border: "1px solid " + (disabled ? C.border : base.bd),
    padding: pad,
    borderRadius: 7,
    fontSize: 13,
    fontWeight: 500,
    cursor: disabled ? "not-allowed" : "pointer",
    display: "inline-flex",
    alignItems: "center",
    gap: 6,
    transition: "background .12s",
    width: full ? "100%" : undefined,
    justifyContent: full ? "center" : undefined,
    opacity: disabled ? 0.6 : 1,
  };
  const hover = disabled ? {} : hoverBg(base.bg, base.hbg);
  return (
    <button type="button" onClick={onClick} disabled={disabled} title={title} data-testid={testId} style={style} {...hover}>
      {icon ? <span style={{ fontFamily: C.mono, fontSize: 12 }}>{icon}</span> : null}
      {label}
    </button>
  );
}
