# Discord Bot
#import discord
from discord.ext import commands, tasks
# Utilities
from dotenv import load_dotenv
import os
# Custom made functions
from analysis import get_new

# Load variables
load_dotenv() # Load in the .env file
TOKEN = os.getenv('DISCORD_TOKEN')
SERVER = os.getenv('DISCORD_SERVER')

# Set Channel variables Variables
data_engineer_channel = 1001934874539733002
data_analist_channel = 1001934911273443448
business_intelligence_channel = 1001934962364268594
product_owner_channel = 1001935003611045959

# Create Bot instance
bot = commands.Bot(command_prefix="$")

# ON ready
@bot.event
async def on_ready():
    print(f"{bot.user.name} is ready")

# Respond on Message
@bot.command(name="new")
async def _new(ctx):
    """
    Command to retrieve all new vacancies based on channel
    """
    # Check for channel
    if ctx.channel.id not in [data_engineer_channel, data_analist_channel, business_intelligence_channel, product_owner_channel]: # Check if in registerd channels
        await ctx.send(f"Command $new works only in Vacancy Channels")
    
    elif ctx.channel.id == data_engineer_channel:
        # Retrieve all new function from MongoDB with help of Support Function
        for vacancy in get_new("Data_Engineer"):
            message = f"Vacature: {vacancy['Job_title']} \nOrganisatie: {vacancy['Organization']} \nLocatie: {vacancy['Location']} \nURL: {vacancy['URL']}"
            await ctx.send(message)

    elif ctx.channel.id == data_analist_channel:
        # Retrieve all new function from MongoDB with help of Support Function
        for vacancy in get_new("Data_Analist"):
            message = f"Vacature: {vacancy['Job_title']} \nOrganisatie: {vacancy['Organization']} \nLocatie: {vacancy['Location']} \nURL: {vacancy['URL']}"
            await ctx.send(message)

    elif ctx.channel.id == business_intelligence_channel:
        # Retrieve all new function from MongoDB with help of Support Function
        for vacancy in get_new("Business_Intelligence"):
            message = f"Vacature: {vacancy['Job_title']} \nOrganisatie: {vacancy['Organization']} \nLocatie: {vacancy['Location']} \nURL: {vacancy['URL']}"
            await ctx.send(message)

    elif ctx.channel.id == product_owner_channel:
        # Retrieve all new function from MongoDB with help of Support Function
        for vacancy in get_new("Product_Owner"):
            message = f"Vacature: {vacancy['Job_title']} \nOrganisatie: {vacancy['Organization']} \nLocatie: {vacancy['Location']} \nURL: {vacancy['URL']}"
            await ctx.send(message)
    else:
        pass


# Respond on Message
@bot.command(name="welcome")
async def _welcome(ctx,arg):
    await ctx.send(f"Hello {arg}, welcome!")

bot.run(TOKEN)
# See: https://stackoverflow.com/questions/57631314/making-a-bot-that-sends-messages-at-a-scheduled-date-with-discord-py?noredirect=1&lq=1
# For more info on how to schedule run
# @tasks.loop(seconds=5)
# async def vacancy_update():
#     message_channel = bot.get_channel(int(CHANNEL_ID))
#     print(f"Got channel {message_channel}")
#     await message_channel.send("Your message")

# @vacancy_update.before_loop
# async def before():
#     await bot.wait_until_ready()
#     print("Finished waiting")

# vacancy_update.start()

