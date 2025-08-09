import asyncio
from etl.load_refs import load_refs
from etl.load_products import load_products

async def main():
    await load_refs()
    await load_products()

if __name__ == "__main__":
    asyncio.run(main())
