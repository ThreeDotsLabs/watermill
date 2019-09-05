package main

import "fmt"

func main() {
	a1 := CreateNewAccount("1")
	a1.Deposit(10)
	if err := a1.Withdraw(3); err != nil {
		panic(err)
	}

	fmt.Println("10-3 = ", a1.Balance())

	repo := &AccountEventSourcedRepository{
		make(map[string][]Event),
	}
	if err := repo.Save(a1); err != nil {
		panic(err)
	}

	a1Repo, err := repo.Find(a1.ID())
	if err != nil {
		panic(err)
	}

	fmt.Println("10-3 = (repo)", a1Repo.Balance())

	a2 := NewAccountFromHistory([]Event{
		AccountCreated{"2"},
		Deposited{15},
		Withdrawed{3},
	})
	fmt.Println("15-3 = ", a2.Balance())
}
