"""
fetch_scryfall.py — Magic: The Gathering cards from Scryfall API → SNF

Usage:
    python fetch_scryfall.py              # Guilds of Ravnica (default)
    python fetch_scryfall.py grn          # same
    python fetch_scryfall.py war          # War of the Spark
    python fetch_scryfall.py --list       # show popular sets

Requirements:
    pip install requests
"""

from __future__ import annotations
import sys
from snf_peirce.base_fetcher import SNFFetcher, fact, facts, facts_from_list, paginate

POPULAR_SETS = {
    "grn": "Guilds of Ravnica",
    "war": "War of the Spark",
    "eld": "Throne of Eldraine",
    "iko": "Ikoria: Lair of Behemoths",
    "neo": "Kamigawa: Neon Dynasty",
    "mom": "March of the Machine",
    "lci": "The Lost Caverns of Ixalan",
    "mkm": "Murders at Karlov Manor",
    "otj": "Outlaws of Thunder Junction",
    "dsk": "Duskmourn: House of Horror",
}

COLOR_NAMES = {"W": "White", "U": "Blue", "B": "Black", "R": "Red", "G": "Green"}

GUILD_NAMES = {
    "U,W": "Azorius", "W,U": "Azorius", "B,U": "Dimir",   "U,B": "Dimir",
    "B,R": "Rakdos",  "R,B": "Rakdos",  "G,R": "Gruul",   "R,G": "Gruul",
    "G,W": "Selesnya","W,G": "Selesnya","B,W": "Orzhov",  "W,B": "Orzhov",
    "R,U": "Izzet",   "U,R": "Izzet",   "B,G": "Golgari", "G,B": "Golgari",
    "R,W": "Boros",   "W,R": "Boros",   "G,U": "Simic",   "U,G": "Simic",
}

SHARD_NAMES = {
    "G,R,W": "Naya", "B,G,U": "Sultai", "B,R,U": "Grixis",
    "B,R,W": "Mardu", "G,U,W": "Bant",
}


class ScryfallFetcher(SNFFetcher):

    def __init__(self, set_code="grn"):
        self.set_code  = set_code.lower()
        self.set_name  = POPULAR_SETS.get(self.set_code, f"Magic Set {set_code.upper()}")
        self.lens_id   = f"scryfall_{self.set_code}_v1"
        self.spoke_dir = f"{self.set_code}_spoke"

    def fetch(self):
        url = (
            f"https://api.scryfall.com/cards/search"
            f"?q=set:{self.set_code}&unique=cards&order=name"
        )
        headers = {"User-Agent": "snf-peirce/0.1.0", "Accept": "application/json"}
        cards = list(paginate(url, headers=headers))
        if not cards:
            raise ValueError(f"No cards found for '{self.set_code.upper()}'.")
        return cards

    def entity_id(self, card):
        return f"scryfall:{card['id']}"

    def translate(self, card):
        eid    = self.entity_id(card)
        colors = card.get("colors", [])
        ci_key = ",".join(sorted(card.get("color_identity", [])))

        type_line = card.get("type_line", "")
        parts     = type_line.split("—")
        types     = parts[0].strip().split() if parts[0].strip() else []
        subtypes  = parts[1].strip().split() if len(parts) > 1 else []

        faction = GUILD_NAMES.get(ci_key) or SHARD_NAMES.get(ci_key)

        return [
            *facts(
                (eid, "what", "name",        card.get("name")),
                (eid, "what", "title",        card.get("name")),
                (eid, "what", "rarity",       card.get("rarity")),
                (eid, "what", "set",          self.set_name),
                (eid, "what", "oracle_text",  card.get("oracle_text")),
            ),
            *facts_from_list(eid, "what", "card_type", types),
            *facts_from_list(eid, "what", "subtype",   subtypes),
            *facts_from_list(eid, "what", "color",     [COLOR_NAMES.get(c, c) for c in colors]),
            *facts_from_list(eid, "what", "color_code", colors),
            *facts_from_list(eid, "what", "keyword",   card.get("keywords", [])),
            *([fact(eid, "what", "guild", faction)] if faction else []),
            *facts(
                (eid, "who",  "artist",    card.get("artist")),
                (eid, "when", "released_at",
                 card["released_at"][:4] if card.get("released_at") else None),
                (eid, "how",  "cmc",
                 str(int(card["cmc"])) if card.get("cmc") is not None else None),
                (eid, "how",  "mana_cost", card.get("mana_cost")),
                (eid, "how",  "power",     card.get("power")),
                (eid, "how",  "toughness", card.get("toughness")),
                (eid, "how",  "loyalty",   card.get("loyalty")),
                (eid, "where","set_code",  card.get("set", "").upper()),
                (eid, "where","collector_number", card.get("collector_number")),
            ),
            *facts_from_list(
                eid, "why", "format_legal",
                [f for f, l in (card.get("legalities") or {}).items() if l != "not_legal"]
            ),
        ]

    def example_queries(self):
        if self.set_code == "grn":
            return [
                'WHAT.guild = "Dimir"',
                'WHAT.color = "Blue" AND WHAT.color = "Black"',
                'WHAT.card_type = "Creature" AND HOW.cmc BETWEEN "1" AND "3"',
                'WHAT.keyword = "Surveil"',
                'WHO.artist = "Seb McKinnon"',
                'WHAT|guild|*',
                'WHO|artist|*',
            ]
        return [
            'WHAT.card_type = "Creature"',
            'WHAT.rarity = "mythic"',
            'HOW.cmc BETWEEN "1" AND "3"',
            'WHAT|keyword|*',
            'WHO|artist|*',
        ]


def main():
    args = sys.argv[1:]
    if "--list" in args:
        print()
        print("  Popular Magic sets:")
        print()
        for code, name in POPULAR_SETS.items():
            print(f"    {code.upper():<6}  {name}")
        print()
        print("  Usage: python fetch_scryfall.py <set_code>")
        print()
        return
    set_code = next((a for a in args if not a.startswith("--")), "grn")
    try:
        ScryfallFetcher(set_code).run()
    except KeyboardInterrupt:
        print("\n\n  Exited.\n")


if __name__ == "__main__":
    main()
