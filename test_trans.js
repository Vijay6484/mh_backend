const Sanscript = require('@indic-transliteration/sanscript');

const dict = {
    "पुणे": "Pune", "हवेली": "Haveli", "मावळ": "Maval", "खेड": "Khed", "जुन्नर": "Junnar", "दौंड": "Daund", "इंदापूर": "Indapur",
    "तर्फे": "Tarf", "शहर": "City", "ग्रामिण": "Rural", "ग्रामीण": "Rural", "नगरपालिका": "Municipal Council", "कँन्टोमेंट": "Cantonment", "कॅन्टोमेंट": "Cantonment"
};

function toEnglishLabel(mr) {
    let s = String(mr ?? '').trim();
    if (!s) return '';

    let words = s.split(/(\s+|[-/,()])/);
    words = words.map(w => {
        if (!w.trim() || /^[-/,()]$/.test(w)) return w;
        if (dict[w]) return dict[w];
        
        let mod = w;
        let suffix = "";
        
        if (mod.endsWith("गांव") || mod.endsWith("गाव")) { mod = mod.replace(/गां?व$/, ""); suffix = "gaon"; }
        else if (mod.endsWith("वाडी")) { mod = mod.replace(/वाडी$/, ""); suffix = "wadi"; }
        else if (mod.endsWith("पूर") || mod.endsWith("पुर")) { mod = mod.replace(/पू?र$/, ""); suffix = "pur"; }
        else if (mod.endsWith("नगर")) { mod = mod.replace(/नगर$/, ""); suffix = "nagar"; }
        else if (mod.endsWith("खुर्द")) { mod = mod.replace(/खुर्द$/, ""); suffix = " Khurd"; }
        else if (mod.endsWith("बुद्रुक") || mod.endsWith("बु ाा")) { mod = mod.replace(/बुद्रुक$/, "").replace(/बु ाा$/, ""); suffix = " Budruk"; }
        
        if (mod === "") return suffix.trim();
        
        let en = Sanscript.t(mod, 'devanagari', 'iast').toLowerCase();
        
        en = en.replace(/ch/g, 'chh').replace(/c/g, 'ch');
        en = en.replace(/ś/g, 'sh').replace(/ṣ/g, 'sh');
        en = en.replace(/ñ/g, 'n').replace(/ṅ/g, 'n').replace(/ṇ/g, 'n').replace(/ṃ/g, 'n').replace(/m̐/g, 'n');
        en = en.replace(/ṭ/g, 't').replace(/ḍ/g, 'd');
        en = en.replace(/ṛ/g, 'ru').replace(/ṝ/g, 'ru');
        en = en.replace(/ḷ/g, 'l').replace(/ḹ/g, 'l');
        en = en.replace(/ā/g, 'a').replace(/ī/g, 'i').replace(/ū/g, 'u').replace(/ē/g, 'e').replace(/ō/g, 'o').replace(/ḥ/g, 'h');
        
        // Schwa deletion
        en = en.replace(/([bcdfghjklmnpqrstvwxyz])a$/g, '$1');
        
        en = en + suffix;
        return en.charAt(0).toUpperCase() + en.slice(1);
    });
    
    return words.join('');
}

const tests = ["आंबेगांव", "अडिवरे", "अवसरी खुर्द", "अवसरी बुद्रुक", "आंबेदरा", "कळंब", "काळेवाडी / कोटमदरा", "गंगापुर खर्द", "चांडोली बुद्रुक", "रावेत", "हिंजवडी"];
tests.forEach(t => console.log(t, "->", toEnglishLabel(t)));
