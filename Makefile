generate-js:
		coffee -c -o lib src/*

publish: generate-js
		npm publish

clean:
		rm lib/*


