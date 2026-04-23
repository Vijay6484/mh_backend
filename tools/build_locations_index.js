#!/usr/bin/env node
/**
 * Reads Backend/locations.json (nested { district: { taluka: [villages] } })
 * and writes Backend/locations_index.json with stable numeric ids + English labels
 * (IT transliteration via Sanscript, title-cased).
 *
 * Usage: node tools/build_locations_index.js
 */
const fs = require('fs');
const path = require('path');
const Sanscript = require('@indic-transliteration/sanscript');

const ROOT = path.join(__dirname, '..');
const INPUT = path.join(ROOT, 'locations.json');
const OUTPUT = path.join(ROOT, 'locations_index.json');

const dict = {
    "पुणे": "Pune", "हवेली": "Haveli", "मावळ": "Maval", "खेड": "Khed", "जुन्नर": "Junnar", "दौंड": "Daund", "इंदापूर": "Indapur",
    "तर्फे": "Tarf", "शहर": "City", "ग्रामिण": "Rural", "ग्रामीण": "Rural", "नगरपालिका": "Municipal Council", "कँन्टोमेंट": "Cantonment", "कॅन्टोमेंट": "Cantonment",
    "खुर्द": "Khurd", "बुद्रुक": "Budruk", "खर्द": "Khurd",
    "कसबा": "Kasba", "एरंडवणा": "Erandwane", "कोथरूड": "Kothrud", "शिवाजीनगर": "Shivajinagar",
    "भवानी": "Bhavani", "नाना": "Nana", "रास्ता": "Rasta", "गुरूवार": "Guruwar", "शुक्रवार": "Shukrawar",
    "शनिवार": "Shaniwar", "रविवार": "Raviwar", "सोमवार": "Somwar", "मंगळवार": "Mangalwar", "बुधवार": "Budhwar",
    "नारायण": "Narayan", "सदाशिव": "Sadashiv", "नवी": "Navi", "गंज": "Ganj",
    "घोरपडी": "Ghorpadi", "खडकी": "Khadki", "येरवडा": "Yerawada", "वानवडी": "Wanawadi", "वाकड": "Wakad",
    "हिंजवडी": "Hinjawadi", "बाणेर": "Baner", "बालेवाडी": "Balewadi", "पाषाण": "Pashan", "औंध": "Aundh",
    "बोपोडी": "Bopodi", "दापोडी": "Dapodi", "पिंपरी": "Pimpri", "चिंचवड": "Chinchwad", "निगडी": "Nigdi",
    "आकुर्डी": "Akurdi", "भोसरी": "Bhosari", "कात्रज": "Katraj", "धनकवडी": "Dhankawadi", "बिबवेवाडी": "Bibwewadi",
    "कोंढवा": "Kondhwa", "हडपसर": "Hadapsar", "मुंढवा": "Mundhwa", "खराडी": "Kharadi", "विमाननगर": "Viman Nagar",
    "कल्याणीनगर": "Kalyani Nagar", "विश्रांतवाडी": "Vishrantwadi", "धानोरी": "Dhanori", "लोहगाव": "Lohegaon",
    "वडगाव": "Wadgaon", "शेरी": "Sheri", "खडकवासला": "Khadakwasla", "सिंहगड": "Sinhagad", "नऱ्हे": "Narhe",
    "धायरी": "Dhayari", "वारजे": "Warje", "कर्वेनगर": "Karvenagar", "डेक्कन": "Deccan", "कॅम्प": "Camp",
    "स्वारगेट": "Swargate", "स्टेशन": "Station", "लोणावळा": "Lonavala", "तळेगाव": "Talegaon", "दाभाडे": "Dabhade",
    "चाकण": "Chakan", "राजगुरुनगर": "Rajgurunagar", "शिरूर": "Shirur", "बारामती": "Baramati", "इंदापुर": "Indapur",
    "दौड": "Daund", "भोर": "Bhor", "वेल्हा": "Velha", "मुळशी": "Mulshi", "पुरंदर": "Purandar", "सासवड": "Saswad",
    "जेजुरी": "Jejuri", "शिरवळ": "Shirwal", "खंडाळा": "Khandala", "महाबळेश्वर": "Mahabaleshwar", "पाचगणी": "Panchgani",
    "वाई": "Wai", "सातारा": "Satara", "कराड": "Karad", "कोरेगाव": "Koregaon", "पाटण": "Patan", "फलटण": "Phaltan",
    "माण": "Man", "खटाव": "Khatav", "जावळी": "Jawali", "महाड": "Mahad", "शिरोळ": "Shirala", "कोल्हापूर": "Kolhapur",
    "उरळी": "Uruli", "कांचन": "Kanchan", "देवाची": "Devachi", "म्हातोबाची": "Mhatobachi", "आळंदी": "Alandi",
    "चिखली": "Chikhali", "तळवडे": "Talawade", "रुपीनगर": "Rupinagar", "थेरगाव": "Thergaon", "काळेवाडी": "Kalewadi",
    "पिंपळे": "Pimple", "सौदागर": "Saudagar", "गुरव": "Gurav", "निलख": "Nilakh", "रहाटणी": "Rahatani",
    "पुनावळे": "Punawale", "किवळे": "Kiwale", "रावेत": "Ravet", "निगडी": "Nigdi", "प्राधिकरण": "Pradhikaran",
    "आकुर्डी": "Akurdi", "चिंचवड": "Chinchwad", "मोहननगर": "Mohannagar", "शाहूनगर": "Shahunagar", "मोरवाडी": "Morwadi",
    "खराळवाडी": "Kharalwadi", "नेहरूनगर": "Nehrunagar", "मासुळकर": "Masulkar", "कॉलनी": "Colony", "अजमेरा": "Ajmera",
    "भोसरी": "Bhosari", "इंद्रायणीनगर": "Indrayaninagar", "मोशी": "Moshi", "चऱ्होली": "Charholi", "दिघी": "Dighi",
    "बोपखेल": "Bopkhel", "दापोडी": "Dapodi", "कासारवाडी": "Kasarwadi", "फुगेवाडी": "Phugewadi", "संगवी": "Sangvi",
    "नवी": "Navi", "जुनी": "Juni", "पिंपरी": "Pimpri", "वाघेरे": "Waghere", "संत": "Sant", "तुकाराम": "Tukaram",
    "नगर": "Nagar", "यशवंतराव": "Yashwantrao", "चव्हाण": "Chavan", "स्मृती": "Smruti", "उद्यान": "Udyan",
    "तळजाई": "Taljai", "पठार": "Pathar", "धनकवडी": "Dhankawadi", "आंबेगाव": "Ambegaon", "जांभूळवाडी": "Jambhulwadi",
    "मांगडेवाडी": "Mangdewadi", "भिलारेवाडी": "Bhilarewadi", "गुजर": "Gujar", "निंबाळकरवाडी": "Nimbalkarwadi",
    "कात्रज": "Katraj", "कोंढवा": "Kondhwa", "येवलेवाडी": "Yewalewadi", "पिसोळी": "Pisoli", "उंड्री": "Undri",
    "हांडेवाडी": "Handewadi", "औताडे": "Autade", "होळकरवाडी": "Holkarwadi", "वडकी": "Wadaki", "फुरसुंगी": "Fursungi",
    "मांजरी": "Manjari", "शेवाळवाडी": "Shewalwadi", "कदमवाकवस्ती": "Kadamwakwasti", "लोणी": "Loni", "काळभोर": "Kalbhor"
};

function toEnglishLabel(mr) {
    let s = String(mr ?? '').trim();
    if (!s) return '';

    // Pre-processing for multi-word suffixes
    s = s.replace(/बु ाा/g, "Budruk");
    s = s.replace(/बु\./g, "Budruk");
    s = s.replace(/खु\./g, "Khurd");

    let words = s.split(/(\s+|[-/,()])/);
    words = words.map(w => {
        if (!w.trim() || /^[-/,()]$/.test(w)) return w;
        if (dict[w]) return dict[w];
        if (w === "Budruk" || w === "Khurd") return w; // already replaced
        
        let mod = w;
        let suffix = "";
        
        if (mod.endsWith("गांव") || mod.endsWith("गाव")) { mod = mod.replace(/गां?व$/, ""); suffix = "gaon"; }
        else if (mod.endsWith("वाडी")) { mod = mod.replace(/वाडी$/, ""); suffix = "wadi"; }
        else if (mod.endsWith("पूर") || mod.endsWith("पुर")) { mod = mod.replace(/पू?र$/, ""); suffix = "pur"; }
        else if (mod.endsWith("नगर")) { mod = mod.replace(/नगर$/, ""); suffix = "nagar"; }
        else if (mod.endsWith("दरा")) { mod = mod.replace(/दरा$/, ""); suffix = "dara"; }
        
        if (mod === "") return suffix.trim().charAt(0).toUpperCase() + suffix.trim().slice(1);
        
        let en = Sanscript.t(mod, 'devanagari', 'iast').toLowerCase();
        
        // Fix IAST characters
        en = en.replace(/ch/g, 'chh').replace(/c/g, 'ch');
        en = en.replace(/ś/g, 'sh').replace(/ṣ/g, 'sh');
        en = en.replace(/ñ/g, 'n').replace(/ṅ/g, 'n').replace(/ṇ/g, 'n').replace(/ṃ/g, 'n').replace(/m̐/g, 'n');
        en = en.replace(/ṭ/g, 't').replace(/ḍ/g, 'd');
        en = en.replace(/ṛ/g, 'ru').replace(/ṝ/g, 'ru');
        en = en.replace(/ḷ/g, 'l').replace(/ḹ/g, 'l').replace(/l̤/g, 'l');
        en = en.replace(/ā/g, 'a').replace(/ī/g, 'i').replace(/ū/g, 'u').replace(/ē/g, 'e').replace(/ō/g, 'o').replace(/ḥ/g, 'h');
        
        // Marathi specific fixes
        en = en.replace(/nb/g, 'mb').replace(/np/g, 'mp');
        en = en.replace(/v/g, 'v');
        
        // Schwa deletion (drop trailing 'a' if it follows a consonant)
        en = en.replace(/([bcdfghjklmnpqrstvwxyz])a$/g, '$1');
        
        en = en + suffix;
        return en.charAt(0).toUpperCase() + en.slice(1);
    });
    
    return words.join('');
}

function main() {
    const raw = fs.readFileSync(INPUT, 'utf8');
    const legacy = JSON.parse(raw);

    const districts = Object.keys(legacy).sort((a, b) => a.localeCompare(b, 'mr'));
    const out = {
        version: 1,
        generated: new Date().toISOString(),
        districts: [],
    };

    let dId = 0;
    for (const dMr of districts) {
        dId += 1;
        const talukaMap = legacy[dMr] || {};
        const talukaNames = Object.keys(talukaMap).sort((a, b) => a.localeCompare(b, 'mr'));
        const talukas = [];
        let tId = 0;
        for (const tMr of talukaNames) {
            tId += 1;
            const villagesMr = Array.isArray(talukaMap[tMr]) ? talukaMap[tMr] : [];
            const villages = [];
            let vId = 0;
            const vSorted = [...villagesMr].sort((a, b) => String(a).localeCompare(String(b), 'mr'));
            for (const vMr of vSorted) {
                vId += 1;
                villages.push({
                    id: vId,
                    mr: vMr,
                    en: toEnglishLabel(vMr),
                });
            }
            talukas.push({
                id: tId,
                mr: tMr,
                en: toEnglishLabel(tMr),
                villages,
            });
        }
        out.districts.push({
            id: dId,
            mr: dMr,
            en: toEnglishLabel(dMr),
            talukas,
        });
    }

    const tmp = OUTPUT + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(out, null, 2), 'utf8');
    fs.renameSync(tmp, OUTPUT);
    console.log(`Wrote ${OUTPUT} (${out.districts.length} districts)`);
}

main();
