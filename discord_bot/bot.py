# Discord Bot
#import discord
from io import BytesIO
from discord.ext import commands, tasks
import discord
from PIL import Image
# Utilities
from dotenv import load_dotenv
import os
# Custom made functions
from analysis import get_new, summary_statistics, location_statistics, skills_statistics

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
# New command
@bot.command(name="new")
async def _new(ctx):
    """
    Command to retrieve all new vacancies based on channel
    """
    # Check for channel
    if ctx.channel.id not in [data_engineer_channel, data_analist_channel, business_intelligence_channel, product_owner_channel]: # Check if in registerd channels
        await ctx.send(f"Command $new works only in Vacancy Channels")
        return 0
    
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

# Summary
@bot.command(name="summary")
async def _summary(ctx):
    """
    Function to retrieve Summary Statics for Function
    """
    if ctx.channel.id not in [data_engineer_channel, data_analist_channel, business_intelligence_channel, product_owner_channel]: 
        # Check if in registerd channels
        await ctx.send(f"Command $summary works only in Vacancy Channels")
        return 0
    
    elif ctx.channel.id == data_engineer_channel:
        # Create Chart
        image_path = summary_statistics("Data_Engineer")
        
    elif ctx.channel.id == data_analist_channel:
        # Create Chart
        image_path = summary_statistics("Data_Analist")

    elif ctx.channel.id == business_intelligence_channel:
        # Create Chart
        image_path = summary_statistics("Business_Intelligence")

    elif ctx.channel.id == product_owner_channel:
        # Create Chart
        image_path = summary_statistics("Product_Owner")

    else:
        pass

    # Open Image
    image = Image.open(image_path)
    # Send Image
    with BytesIO() as image_binary:
        image.save(image_binary, "PNG")
        image_binary.seek(0)
        await ctx.send(file=discord.File(fp=image_binary, filename="Summary_stats.png"))

# Location
@bot.command(name="location")
async def _location(ctx):
    """
    Function to send Location Summary
    """
    if ctx.channel.id not in [data_engineer_channel, data_analist_channel, business_intelligence_channel, product_owner_channel]: 
        # Check if in registered channels
        await ctx.send(f"Command $location works only in Vacancy Channels")
        return 0
    
    elif ctx.channel.id == data_engineer_channel:
        # Create Chart
        image_path = location_statistics("Data_Engineer")
        
    elif ctx.channel.id == data_analist_channel:
        # Create Chart
        image_path = location_statistics("Data_Analist")

    elif ctx.channel.id == business_intelligence_channel:
        # Create Chart
        image_path = location_statistics("Business_Intelligence")

    elif ctx.channel.id == product_owner_channel:
        # Create Chart
        image_path = location_statistics("Product_Owner")
    else:
        pass

    # Open Image
    image = Image.open(image_path)
    # Send Image
    with BytesIO() as image_binary:
        image.save(image_binary, "PNG")
        image_binary.seek(0)
        await ctx.send(file=discord.File(fp=image_binary, filename="Location_stats.png"))

# skills
@bot.command(name="skills")
async def _skills(ctx):
    """
    Function to send skills Summary
    """
    if ctx.channel.id not in [data_engineer_channel, data_analist_channel, business_intelligence_channel, product_owner_channel]: 
        # Check if in registered channels
        await ctx.send(f"Command $skills works only in Vacancy Channels")
        return 0
    
    elif ctx.channel.id == data_engineer_channel:
        # Create Chart
        image_path = skills_statistics("Data_Engineer") # unable to create image in Docker-Compose
        
    elif ctx.channel.id == data_analist_channel:
        # Create Chart
        image_path = skills_statistics("Data_Analist")

    elif ctx.channel.id == business_intelligence_channel:
        # Create Chart
        image_path = skills_statistics("Business_Intelligence")

    elif ctx.channel.id == product_owner_channel:
        # Create Chart
        image_path = skills_statistics("Product_Owner")
    else:
        pass

    # Open Image
    image = Image.open(image_path)
    # Send Image
    with BytesIO() as image_binary:
        image.save(image_binary, "PNG")
        image_binary.seek(0)
        await ctx.send(file=discord.File(fp=image_binary, filename="skills_stats.png"))

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

