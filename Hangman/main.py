
finish = False # Setting some global booleans.
valid = False

while not valid: # Every one of this kind of loop is to make sure user doesn't input something that doesn't work. 
    main_word = input("Enter the word to play with: ")
    if not main_word.isalpha():
        print("That is an invalid input. Please try again.")
    else:
        valid = True # Once they input something valid, the loop ends.

print("\n"*100) # This creates some space so the answer isn't just sitting on screen. 
main_word = str.lower(main_word) # Setting up some variables and lists to be called later. 
real_letters = list(main_word)
letter_num = len(real_letters)
guess_list = []

for i in range(letter_num): # Creating the visible list of letter placement. Correct guesses will go in ehre.
    guess_list.append("__")

letter_num = str(letter_num)
print(guess_list)
wrong_list = [] # This list will hold user's incorrect answers. 
print("Thank you. There are " + letter_num + " letters in the word. Try not to guess wrong 6 times!")
letter_num = int(letter_num)
wrong_counter = 1 # Setting the starting number of incorrect guesses.
while not finish: # Note that case doesn't matter for all these since we convert to lower and upper as needed.
    valid = False
    while not valid:
        guess = input("Please Guess a Letter: ")
        if not guess.isalpha():
            print("You have inputted an invalid character. Please try again.")
        else:
            guess = str.lower(guess) # This extra step makes sure that they only put in a character, not a word. 
            if not len(list(guess)) == 1: # There's probably a way to do this with 'char', but this came to me first so I used it.
                print("You have inputted an invalid character. Please try again.")
            else:
                valid = True

    if guess in real_letters: # Checking if the guess is in the word. 
        guess = str.lower(guess)
        print("That is a correct guess. That letter appears in the following positions:")
        for positions in range(letter_num): # This adds the correct guess in all its positions in the word. 
            if guess == real_letters[positions]:
                guess = str.upper(guess)
                guess_list[positions] = guess
                guess = str.lower(guess)
        print(guess_list) # Shows the user where their letters ended up. 
        valid = False
        while not valid:
            would_guess= input("Would you like to guess the word? Please type 'Yes' or 'No'. ") # This checks if they want to guess the word or not. 
            if not would_guess.isalpha():
                print("That is an invalid input. Please try again.")
            else:
                would_guess = str.lower(would_guess)
                valid = True
        if would_guess == "yes":
            valid = False
            while not valid:
                word_guess = input("Please guess the word: ")
                if not word_guess.isalpha():
                    print("That is an invalid input. Please try again.")
                else:
                    word_guess = str.lower(word_guess)
                    if word_guess == main_word:
                        print("You are correct! Thanks for playing!")
                        valid = True
                        finish = True
                    else:
                        print("I'm sorry, that isn't correct.")
                        valid = True

        elif would_guess == "no":
            print("Very well, then.")
        else:
            print("Could not understand your answer. Please guess another letter.")

    else: # This is where incorrect answers go. 
        if wrong_counter == 6: # If they've guessed wrong too many times, they lose. 
            print("That is incorrect. You are out of guesses. The word was '" + main_word + "'. Thanks for playing!")
            finish = True
        else: # Increments the guess counter, adds the incorrect guess to the list, and displays all their incorrect guesses. 
            wrong_counter = str(wrong_counter)
            wrong_list.append(guess)
            print("That is incorrect. You have answered incorrectly " + wrong_counter + " times. You have guessed:")
            print(wrong_list)
            wrong_counter = int(wrong_counter)
            wrong_counter = wrong_counter + 1
