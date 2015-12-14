generate-js:
		coffee -c -o lib src/*

publish: generate-js
		npm version patch
		npm publish

clean:
		rm lib/*


