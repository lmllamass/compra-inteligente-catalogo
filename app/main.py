def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["ngrams","digits","brands","families","trigrams"], help="Estrategia única")
    p.add_argument("--modes", type=str, help="CSV de estrategias para loop: brands,families,ngrams,digits,trigrams")
    p.add_argument("--loop", action="store_true", help="Ejecutar en bucle infinito (rotando estrategias)")
    p.add_argument("--idle-sleep", type=int, default=300, help="Espera entre ciclos (segundos)")
    args = p.parse_args()

    if args.loop:
        modes = parse_modes(args.modes) if args.modes else ["brands","families","ngrams","digits"]
        run_cycle(modes, idle_sleep=args.idle_sleep)
    else:
        if not args.mode:
            raise SystemExit("Debes indicar --mode o usar --loop con --modes")
        asyncio.run(run_strategy(args.mode))