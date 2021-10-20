# Web-Information-Retrieval
![](cover_image.jpg)
© www.ics.uci.edu/~djp3/classes/2009_01_02_INF141/.  All rights reserved


This is an ongoing project as part of the course Web Information Retrieval taught by Professor Sara Cohen.
All datasets were taken from [Stanford Large Network Dataset Collection](http://snap.stanford.edu/data/index.html).

## The game
* Link-a-Pix is a board game puzzle that form whimsical pixel-art pictures when solved.     

* Each puzzle consists of a grid containing numbers in various places.    
  Every number, except for the 1’s, is half of a pair.    
  
* The purpose is to reveal a hidden picture by linking the pairs and painting the paths so that    
  the number of squares in the path, including the squares at the ends,     
  equals the value of the numbers being linked together.    


## Solutions
The solution of the game is finding a series of legal paths under the constraints   
in order to get the final picture (each board has a unique solution).     
In order to solve our problem we used multiple solutions:   

* CSP: Backtracking algorithm

* Search algorithms: DFS, BFS, UCS and A*

* Machine learning algorithm 


## Usage

```
download runner.zip
open Link-A-Pix.exe
choose a board and your desired tactic
click Run!
```

