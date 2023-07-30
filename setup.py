import setuptools


setuptools.setup(

    name="signalbot",

    version="1.0.0",

    author="filipre",

    url="https://github.com/filipre/signalbot",

    packages=setuptools.find_packages(),

    classifiers=[

        "Programming Language :: Python :: 3",

        "License :: OSI Approved :: MIT License",

        "Operating System :: OS Independent",

    ],

    python_requires='>=3.6',
    
    install_requires=['websocket', 'aiohttp', 'APScheduler', 'redis']

)
